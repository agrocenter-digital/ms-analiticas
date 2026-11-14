package com.agrocenter.analiticas.operacion;

import static org.assertj.core.api.Assertions.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.DriverManager;
import java.util.HexFormat;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

/** Exclusivamente para BD temporal local de pruebas. */
@EnabledIfEnvironmentVariable(named = "ANALYTICS_OPERATOR_PG_TEST", matches = "true")
class ResolucionCommandPostgresTest {
    @TempDir Path temp;
    @Test void operadorRestringidoCorrigeUnaVezSinModificarOriginal() throws Exception {
        String url = System.getenv("DB_URL");
        if (url == null || !url.matches("jdbc:postgresql://localhost:[0-9]+/analiticas_test"))
            throw new IllegalStateException("La prueba requiere BD temporal local analiticas_test");
        String seq = UUID.randomUUID().toString();
        String id = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(("negocio\nshard\n" + seq + "\n0").getBytes(StandardCharsets.UTF_8)));
        String eventId = "operador-" + UUID.randomUUID();
        Path archivo = temp.resolve("corregido.json");
        Files.writeString(archivo, ("""
                {"eventId":"%s","eventType":"VentaConfirmada","schemaVersion":1,"occurredAt":"2026-11-12T22:00:00Z",
                 "payload":{"ventaId":1,"total":2.00,"items":[{"productoId":1,"sku":"S","cantidad":2,"precioUnitario":1.00,"subtotal":2.00}]}}
                """).formatted(eventId));
        try (var admin = DriverManager.getConnection(url, "postgres", "localtest")) {
            try (var insert = admin.prepareStatement("""
                    INSERT INTO eventos_cuarentena(id,stream,shard,sequence_number,sub_sequence_number,payload_base64,tipo_error,created_at)
                    VALUES (?, 'negocio','shard',?,0,'e21hbA==','EVENTO_INVALIDO',now())
                    """)) { insert.setString(1, id); insert.setString(2, seq); insert.executeUpdate(); }
            try {
                String[] args = {"--confirmo-consumidores-detenidos", id, archivo.toString(), "revisado"};
                String anterior = System.getProperty("ANALYTICS_CONSUMER_DB_USERNAME");
                try {
                    System.setProperty("ANALYTICS_CONSUMER_DB_USERNAME", "analiticas_operator_test");
                    assertThatThrownBy(() -> ResolucionCommand.ejecutar(args)).isInstanceOf(IllegalStateException.class);
                } finally {
                    if (anterior == null) System.clearProperty("ANALYTICS_CONSUMER_DB_USERNAME");
                    else System.setProperty("ANALYTICS_CONSUMER_DB_USERNAME", anterior);
                }
                ResolucionCommand.ejecutar(args);
                assertThatThrownBy(() -> ResolucionCommand.ejecutar(args)).isInstanceOf(IllegalStateException.class);
                try (var select = admin.prepareStatement("SELECT resuelto_por,event_id_corregido,payload_base64 FROM eventos_cuarentena WHERE id=?")) {
                    select.setString(1, id);
                    try (var rs = select.executeQuery()) {
                        assertThat(rs.next()).isTrue();
                        assertThat(rs.getString(1)).isEqualTo("db:analiticas_operator_test");
                        assertThat(rs.getString(2)).isEqualTo(eventId);
                        assertThat(rs.getString(3)).isEqualTo("e21hbA==");
                    }
                }
                try (var q = admin.createStatement(); var rs = q.executeQuery("SELECT ventas FROM metricas_ventas_diarias WHERE fecha='2026-11-12'")) {
                    assertThat(rs.next()).isTrue(); assertThat(rs.getLong(1)).isEqualTo(1);
                }
            } finally {
                try (var q = admin.createStatement()) {
                    q.executeUpdate("DELETE FROM eventos_cuarentena");
                    q.executeUpdate("DELETE FROM eventos_procesados");
                    q.executeUpdate("DELETE FROM metricas_productos_diarias");
                    q.executeUpdate("DELETE FROM metricas_ventas_diarias");
                }
            }
        }
    }
}
