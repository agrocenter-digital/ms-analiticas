package com.agrocenter.analiticas.kinesis;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.agrocenter.analiticas.eventos.AnaliticasProcessor;
import com.agrocenter.analiticas.eventos.CuarentenaService;
import com.agrocenter.analiticas.eventos.EventoInvalidoException;
import com.agrocenter.analiticas.eventos.ResolucionCuarentenaService;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.support.TransactionTemplate;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

@SpringBootTest(properties = "agrocenter.analiticas.resolucion.enabled=true")
@EnabledIfEnvironmentVariable(named = "ANALYTICS_PG_TEST", matches = "true")
class ResolucionCuarentenaPostgresTest {
    @Autowired ResolucionCuarentenaService resolver;
    @Autowired CuarentenaService cuarentena;
    @Autowired AnaliticasProcessor processor;
    @Autowired JdbcTemplate jdbc;
    @Autowired TransactionTemplate transactions;
    @MockitoBean JwtDecoder decoder;
    private static final byte[] ORIGINAL = "{mal".getBytes(StandardCharsets.UTF_8);

    @BeforeEach void preparar() {
        limpiar();
        cuarentena.guardar("negocio", "shard", "123", 2, ORIGINAL, "EVENTO_INVALIDO");
    }
    @AfterEach void limpiar() {
        jdbc.update("DELETE FROM eventos_cuarentena");
        jdbc.update("DELETE FROM eventos_procesados");
        jdbc.update("DELETE FROM metricas_ventas_diarias");
        jdbc.update("DELETE FROM metricas_productos_diarias");
        jdbc.update("DELETE FROM metricas_b2b_diarias");
        jdbc.update("DELETE FROM metricas_alertas_stock_diarias");
        jdbc.update("DELETE FROM alertas_stock");
    }

    @Test void corrigeAtomicaYReleeSinDuplicar() throws Exception {
        String id = id();
        resolver.resolver(id, "admin", "correccion revisada", venta("corregido-1"));
        assertThat(cuarentena.resuelta("negocio", "shard", "123", 2, ORIGINAL)).isTrue();
        assertThat(cuarentena.resuelta("negocio", "shard", "123", 2, "otro".getBytes(StandardCharsets.UTF_8))).isFalse();
        assertThat(jdbc.queryForObject("SELECT resuelto_por FROM eventos_cuarentena WHERE id=?", String.class, id)).isEqualTo("admin");
        assertThat(jdbc.queryForObject("SELECT ventas FROM metricas_ventas_diarias", Long.class)).isEqualTo(1);
        assertThatThrownBy(() -> resolver.resolver(id, "admin", "otra", venta("corregido-2"))).isInstanceOf(IllegalStateException.class);
        var checkpoint = mock(RecordProcessorCheckpointer.class);
        var p = new AnaliticasShardProcessor("negocio", processor, cuarentena, new EstadoConsumidores.Stream(), () -> false, () -> {}, 100);
        p.initialize(InitializationInput.builder().shardId("shard").build());
        var record = KinesisClientRecord.builder().sequenceNumber("123").subSequenceNumber(2).data(ByteBuffer.wrap(ORIGINAL)).build();
        p.processRecords(ProcessRecordsInput.builder().records(List.of(record)).checkpointer(checkpoint).build());
        verify(checkpoint).checkpoint();
        assertThat(jdbc.queryForObject("SELECT ventas FROM metricas_ventas_diarias", Long.class)).isEqualTo(1);
    }

    @Test void invalidoDuplicadoOIrrelevanteNoResuelveYRollbackRevierte() {
        String id = id();
        assertThatThrownBy(() -> resolver.resolver(id, "admin", "revisado", ORIGINAL)).isInstanceOf(EventoInvalidoException.class);
        assertThatThrownBy(() -> resolver.resolver(id, "admin", "revisado", "{\"eventType\":\"Otro\"}".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> transactions.executeWithoutResult(status -> {
            resolver.resolver(id, "admin", "revisado", venta("rollback"));
            throw new IllegalStateException("rollback");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(cuarentena.resuelta("negocio", "shard", "123", 2, ORIGINAL)).isFalse();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM eventos_procesados", Long.class)).isZero();
        processor.procesar(venta("ya-existe"));
        assertThatThrownBy(() -> resolver.resolver(id, "admin", "revisado", venta("ya-existe"))).isInstanceOf(IllegalStateException.class);
        assertThat(cuarentena.resuelta("negocio", "shard", "123", 2, ORIGINAL)).isFalse();
    }

    @Test void correccionesSimultaneasSoloAdmitenUna() throws Exception {
        String id = id(); var inicio = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var a = executor.submit(() -> intentar(inicio, id, "a"));
            var b = executor.submit(() -> intentar(inicio, id, "b"));
            inicio.countDown();
            assertThat(List.of(a.get(20, TimeUnit.SECONDS), b.get(20, TimeUnit.SECONDS))).containsExactlyInAnyOrder(true, false);
        }
        assertThat(jdbc.queryForObject("SELECT ventas FROM metricas_ventas_diarias", Long.class)).isEqualTo(1);
    }

    private boolean intentar(CountDownLatch inicio, String id, String eventId) throws InterruptedException {
        inicio.await();
        try { resolver.resolver(id, "admin", "revisado", venta(eventId)); return true; }
        catch (IllegalStateException e) { return false; }
    }
    private String id() { return jdbc.queryForObject("SELECT id FROM eventos_cuarentena", String.class); }
    private byte[] venta(String id) {
        return ("""
                {"eventId":"%s","eventType":"VentaConfirmada","schemaVersion":1,"occurredAt":"2026-11-12T22:00:00Z",
                 "payload":{"ventaId":1,"total":2.00,"items":[{"productoId":1,"sku":"S","cantidad":2,"precioUnitario":1.00,"subtotal":2.00}]}}
                """).formatted(id).getBytes(StandardCharsets.UTF_8);
    }
}
