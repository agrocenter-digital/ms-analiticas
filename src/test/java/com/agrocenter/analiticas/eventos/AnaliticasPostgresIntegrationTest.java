package com.agrocenter.analiticas.eventos;

import static org.assertj.core.api.Assertions.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import com.agrocenter.analiticas.consulta.MetricasRepository;

@SpringBootTest
@EnabledIfEnvironmentVariable(named = "ANALYTICS_PG_TEST", matches = "true")
class AnaliticasPostgresIntegrationTest {
    @Autowired AnaliticasProcessor processor;
    @Autowired JdbcTemplate jdbc;
    @Autowired TransactionTemplate transactions;
    @Autowired MetricasRepository consultas;
    @MockitoBean JwtDecoder decoder;

    @BeforeEach void limpiar() {
        jdbc.update("DELETE FROM eventos_procesados");
        jdbc.update("DELETE FROM metricas_ventas_diarias");
        jdbc.update("DELETE FROM metricas_productos_diarias");
        jdbc.update("DELETE FROM metricas_b2b_diarias");
        jdbc.update("DELETE FROM metricas_alertas_stock_diarias");
        jdbc.update("DELETE FROM alertas_stock");
    }

    @Test void duplicadosConcurrentesAgreganUnaSolaVenta() throws Exception {
        byte[] evento = venta("venta-1", "2026-11-12T22:00:00Z", "1.25", "2.50");
        var inicio = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var a = executor.submit(() -> { inicio.await(); return processor.procesar(evento); });
            var b = executor.submit(() -> { inicio.await(); return processor.procesar(evento); });
            inicio.countDown();
            assertThat(List.of(a.get(20, TimeUnit.SECONDS), b.get(20, TimeUnit.SECONDS))).containsExactlyInAnyOrder(true, false);
        }
        assertThat(jdbc.queryForObject("SELECT ventas FROM metricas_ventas_diarias WHERE fecha='2026-11-12'", Long.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT monto FROM metricas_ventas_diarias", java.math.BigDecimal.class)).isEqualByComparingTo("2.50");
        assertThat(jdbc.queryForObject("SELECT unidades FROM metricas_productos_diarias", Long.class)).isEqualTo(2);
    }

    @Test void eventoTardioSeAgregaAlDiaUtcDeOcurrencia() {
        processor.procesar(venta("v2", "2026-11-13T00:30:00Z", "1.00", "2.00"));
        processor.procesar(venta("v1", "2026-11-12T23:30:00Z", "2.00", "4.00"));
        assertThat(jdbc.queryForObject("SELECT monto FROM metricas_ventas_diarias WHERE fecha='2026-11-12'", java.math.BigDecimal.class)).isEqualByComparingTo("4.00");
        assertThat(jdbc.queryForObject("SELECT monto FROM metricas_ventas_diarias WHERE fecha='2026-11-13'", java.math.BigDecimal.class)).isEqualByComparingTo("2.00");
    }

    @Test void eventosDistintosConcurrentesSumanSinPerderActualizaciones() throws Exception {
        var inicio = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var a = executor.submit(() -> { inicio.await(); return processor.procesar(venta("v-a", "2026-11-12T22:00:00Z", "1.00", "2.00")); });
            var b = executor.submit(() -> { inicio.await(); return processor.procesar(venta("v-b", "2026-11-12T22:00:00Z", "1.00", "2.00")); });
            inicio.countDown();
            assertThat(a.get(20, TimeUnit.SECONDS)).isTrue();
            assertThat(b.get(20, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(jdbc.queryForObject("SELECT ventas FROM metricas_ventas_diarias", Long.class)).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT unidades FROM metricas_productos_diarias", Long.class)).isEqualTo(4);
        assertThat(jdbc.queryForObject("SELECT monto FROM metricas_ventas_diarias", java.math.BigDecimal.class)).isEqualByComparingTo("4.00");
    }

    @Test void rollbackNoDejaMarcaNiAgregado() {
        assertThatThrownBy(() -> transactions.executeWithoutResult(status -> {
            processor.procesar(venta("v1", "2026-11-12T22:00:00Z", "1.00", "2.00"));
            throw new IllegalStateException("rollback");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM eventos_procesados", Long.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM metricas_ventas_diarias", Long.class)).isZero();
    }

    @Test void consultasYTransicionesNoCuentanComoVentas() {
        processor.procesar(b2b("b1", "ConsultaB2BCreada", "PENDIENTE", null));
        processor.procesar(b2b("b2", "ConsultaB2BEstadoActualizado", "RESPONDIDO", "PENDIENTE"));
        assertThat(jdbc.queryForObject("SELECT consultas_creadas FROM metricas_b2b_diarias", Long.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT cambios_estado FROM metricas_b2b_diarias", Long.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM metricas_ventas_diarias", Long.class)).isZero();
    }

    @Test void idConContenidoDiferenteNoSeDescarta() {
        processor.procesar(venta("v1", "2026-11-12T22:00:00Z", "1.00", "2.00"));
        assertThatThrownBy(() -> processor.procesar(venta("v1", "2026-11-12T22:00:00Z", "2.00", "4.00")))
                .isInstanceOf(IllegalStateException.class);
        assertThat(jdbc.queryForObject("SELECT ventas FROM metricas_ventas_diarias", Long.class)).isEqualTo(1);
    }

    @Test void alertaTardiaNoSobrescribeUltimaAlertaYDuplicadoNoInflaConteo() {
        byte[] reciente = alerta("s2", "2026-11-13T22:00:00Z", 1);
        processor.procesar(reciente);
        assertThat(processor.procesar(reciente)).isFalse();
        processor.procesar(alerta("s1", "2026-11-12T22:00:00Z", 2));
        assertThat(jdbc.queryForObject("SELECT stock_actual FROM alertas_stock WHERE producto_id=10", Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT sum(alertas) FROM metricas_alertas_stock_diarias", Long.class)).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM alertas_stock", Long.class)).isEqualTo(1);
    }

    @Test void consultasFiltranRangosYExponenUltimaAlertaSinAfirmarEstadoActual() {
        processor.procesar(venta("v1", "2026-11-12T22:00:00Z", "1.25", "2.50"));
        processor.procesar(venta("v2", "2026-11-13T22:00:00Z", "2.00", "4.00"));
        processor.procesar(b2b("b1", "ConsultaB2BCreada", "PENDIENTE", null));
        processor.procesar(alerta("s1", "2026-11-12T23:00:00Z", 1));
        var desde = java.time.LocalDate.parse("2026-11-12");
        var hasta = java.time.LocalDate.parse("2026-11-13");
        var resumen = consultas.resumen(desde, hasta);
        assertThat(resumen.ventas()).isEqualTo(1);
        assertThat(resumen.monto()).isEqualByComparingTo("2.50");
        assertThat(resumen.consultasCreadas()).isEqualTo(1);
        assertThat(resumen.alertasEmitidas()).isEqualTo(1);
        assertThat(consultas.ventas(desde, hasta, 0, 10)).hasSize(1);
        assertThat(consultas.productos(desde, hasta, 0, 10)).singleElement().satisfies(p -> assertThat(p.unidades()).isEqualTo(2));
        assertThat(consultas.b2b(desde, hasta, 0, 10)).hasSize(1);
        assertThat(consultas.alertas(0, 10)).singleElement().satisfies(a -> assertThat(a.stockAlDetectar()).isEqualTo(1));
    }

    private byte[] venta(String id, String fecha, String precio, String total) {
        return ("""
                {"eventId":"%s","eventType":"VentaConfirmada","schemaVersion":1,"occurredAt":"%s",
                 "payload":{"ventaId":1,"total":%s,"items":[{"productoId":10,"sku":"SKU","cantidad":2,"precioUnitario":%s,"subtotal":%s}]}}
                """).formatted(id, fecha, total, precio, total).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] b2b(String id, String tipo, String actual, String anterior) {
        return ("""
                {"eventId":"%s","eventType":"%s","schemaVersion":1,"occurredAt":"2026-11-12T22:00:00Z",
                 "payload":{"consultaId":1,"estadoActual":"%s","estadoAnterior":%s}}
                """).formatted(id, tipo, actual, anterior == null ? "null" : "\"" + anterior + "\"").getBytes(StandardCharsets.UTF_8);
    }

    private byte[] alerta(String id, String fecha, int stock) {
        return ("""
                {"eventId":"%s","eventType":"InventarioCritico","schemaVersion":1,"occurredAt":"%s",
                 "payload":{"productoId":10,"sku":"SKU","stockActual":%d,"stockMinimo":3}}
                """).formatted(id, fecha, stock).getBytes(StandardCharsets.UTF_8);
    }
}
