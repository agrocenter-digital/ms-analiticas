package com.agrocenter.analiticas.operacion;

import com.agrocenter.analiticas.AnaliticasApplication;
import com.agrocenter.analiticas.eventos.ResolucionCuarentenaService;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.jdbc.core.JdbcTemplate;

/** Proceso administrativo separado: no inicia HTTP, KCL ni migraciones. */
public final class ResolucionCommand {
    private ResolucionCommand() { }
    public static void main(String[] args) throws Exception {
        ejecutar(args);
        System.out.println("Resolucion registrada. Compruebe el resultado antes de reiniciar el consumidor.");
    }

    static void ejecutar(String[] args) throws Exception {
        Entrada entrada = Entrada.parse(args);
        if (!Files.isRegularFile(entrada.archivo()) || Files.size(entrada.archivo()) > 1024 * 1024)
            throw new IllegalArgumentException("Archivo corregido inexistente o mayor a 1 MiB");
        byte[] bytes = Files.readAllBytes(entrada.archivo());
        if (bytes.length == 0 || bytes.length > 1024 * 1024) throw new IllegalArgumentException("Archivo vacio o demasiado grande");
        String url = variable("DB_URL"), usuario = variable("DB_USERNAME"), password = variable("DB_PASSWORD");
        String consumidor = variable("ANALYTICS_CONSUMER_DB_USERNAME");
        try (ConfigurableApplicationContext context = new SpringApplicationBuilder(AnaliticasApplication.class)
                .web(WebApplicationType.NONE).profiles("analiticas-resolucion")
                .initializers(app -> app.getEnvironment().getPropertySources().addFirst(new MapPropertySource("resolucionAislada", Map.of(
                        "spring.main.web-application-type", "none", "spring.flyway.enabled", "false",
                        "spring.datasource.url", url, "spring.datasource.username", usuario, "spring.datasource.password", password,
                        "agrocenter.analiticas.kinesis.enabled", "false", "agrocenter.analiticas.resolucion.enabled", "true"))))
                .run()) {
            var jdbc = context.getBean(JdbcTemplate.class);
            String operador = jdbc.queryForObject("SELECT current_user", String.class);
            Boolean privilegiado = jdbc.queryForObject("SELECT rolsuper OR rolcreaterole OR rolcreatedb OR rolreplication OR rolbypassrls FROM pg_roles WHERE rolname=current_user", Boolean.class);
            if (operador == null || operador.equals(consumidor) || Boolean.TRUE.equals(privilegiado))
                throw new IllegalStateException("Rol operador debe ser distinto al consumidor y no privilegiado");
            String[] tablas = {"eventos_procesados", "eventos_cuarentena", "metricas_ventas_diarias", "metricas_productos_diarias",
                    "metricas_b2b_diarias", "metricas_alertas_stock_diarias", "alertas_stock"};
            for (String tabla : tablas) {
                Boolean propietario = jdbc.queryForObject("SELECT pg_get_userbyid(relowner) = current_user FROM pg_class WHERE oid = to_regclass(?)", Boolean.class, "public." + tabla);
                if (Boolean.TRUE.equals(propietario)) throw new IllegalStateException("Rol operador no puede ser propietario de las tablas");
                for (String permiso : new String[] {"DELETE", "TRUNCATE"})
                    if (Boolean.TRUE.equals(jdbc.queryForObject("SELECT has_table_privilege(current_user, ?, ?)", Boolean.class, "public." + tabla, permiso)))
                        throw new IllegalStateException("Rol operador tiene permisos excesivos");
            }
            if (Boolean.TRUE.equals(jdbc.queryForObject("SELECT has_any_column_privilege(current_user, 'public.eventos_procesados', 'UPDATE')", Boolean.class)))
                throw new IllegalStateException("Rol operador no puede modificar marcas de eventos procesados");
            for (String columna : new String[] {"id", "stream", "shard", "sequence_number", "sub_sequence_number", "payload_base64", "tipo_error", "created_at"})
                if (Boolean.TRUE.equals(jdbc.queryForObject("SELECT has_column_privilege(current_user, 'public.eventos_cuarentena', ?, 'UPDATE')", Boolean.class, columna)))
                    throw new IllegalStateException("Rol operador no puede modificar el registro original");
            context.getBean(ResolucionCuarentenaService.class).resolver(entrada.id(), "db:" + operador, entrada.motivo(), bytes);
        }
    }
    private static String variable(String nombre) {
        String valor = System.getProperty(nombre, System.getenv(nombre));
        if (valor == null || valor.isBlank()) throw new IllegalStateException("Se requiere " + nombre);
        return valor;
    }
    record Entrada(String id, Path archivo, String motivo) {
        static Entrada parse(String[] args) {
            if (args.length != 4 || !"--confirmo-consumidores-detenidos".equals(args[0]))
                throw new IllegalArgumentException("Uso: --confirmo-consumidores-detenidos <id> <archivo-json> <motivo>");
            if (!args[1].matches("[0-9a-f]{64}") || args[3].isBlank() || args[3].length() > 500)
                throw new IllegalArgumentException("Identificador o motivo invalidos");
            return new Entrada(args[1], Path.of(args[2]), args[3]);
        }
    }
}
