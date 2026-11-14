package com.agrocenter.analiticas.eventos;

import static org.assertj.core.api.Assertions.assertThat;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/** Ejecutar exclusivamente sobre una base PostgreSQL temporal vacía y dedicada. */
@SpringBootTest
@EnabledIfEnvironmentVariable(named = "CONTRATOS_DIR", matches = ".+")
class ContratoProductoresPostgresTest {
    @Autowired AnaliticasProcessor processor;
    @Autowired JdbcTemplate jdbc;
    @MockitoBean JwtDecoder decoder;

    @Test void agregaVentaYAlertaRealesSinDuplicarlas() throws Exception {
        var dir = Path.of(System.getenv("CONTRATOS_DIR"));
        var venta = Files.readAllBytes(dir.resolve("venta.json"));
        var stock = Files.readAllBytes(dir.resolve("stock.json"));
        assertThat(processor.procesar(venta)).isTrue();
        assertThat(processor.procesar(venta)).isFalse();
        assertThat(processor.procesar(stock)).isTrue();
        assertThat(processor.procesar(stock)).isFalse();
        for (var archivo : java.util.List.of("b2b-creada.json", "b2b-cambio.json")) {
            var bytes = Files.readAllBytes(dir.resolve(archivo));
            assertThat(processor.procesar(bytes)).isTrue();
            assertThat(processor.procesar(bytes)).isFalse();
        }
        for (var tipo : java.util.List.of("CompraCreada", "CompraConfirmada", "CompraCompletada", "CompraCancelada", "ReabastecimientoSolicitado")) {
            assertThat(processor.procesar(Files.readAllBytes(dir.resolve(tipo + ".json")))).isFalse();
        }
        assertThat(jdbc.queryForObject("SELECT SUM(ventas) FROM metricas_ventas_diarias", Long.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT SUM(monto) FROM metricas_ventas_diarias", java.math.BigDecimal.class)).isEqualByComparingTo("2501.00");
        assertThat(jdbc.queryForObject("SELECT SUM(alertas) FROM metricas_alertas_stock_diarias", Long.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT SUM(consultas_creadas) FROM metricas_b2b_diarias", Long.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT SUM(cambios_estado) FROM metricas_b2b_diarias", Long.class)).isEqualTo(1);
    }
}
