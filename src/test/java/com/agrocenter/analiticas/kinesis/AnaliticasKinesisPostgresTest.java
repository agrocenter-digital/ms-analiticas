package com.agrocenter.analiticas.kinesis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.agrocenter.analiticas.eventos.AnaliticasProcessor;
import com.agrocenter.analiticas.eventos.CuarentenaService;
import com.agrocenter.analiticas.consulta.ConsultaCuarentenaRepository;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

@SpringBootTest
@EnabledIfEnvironmentVariable(named = "ANALYTICS_PG_TEST", matches = "true")
class AnaliticasKinesisPostgresTest {
    @Autowired AnaliticasProcessor handler;
    @Autowired CuarentenaService cuarentena;
    @Autowired ConsultaCuarentenaRepository consultaCuarentena;
    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectProvider<AnaliticasKinesisConsumers> consumers;
    @MockitoBean JwtDecoder decoder;

    @BeforeEach void limpiar() {
        jdbc.update("DELETE FROM eventos_cuarentena");
        jdbc.update("DELETE FROM eventos_procesados");
        jdbc.update("DELETE FROM metricas_ventas_diarias");
        jdbc.update("DELETE FROM metricas_productos_diarias");
        jdbc.update("DELETE FROM metricas_b2b_diarias");
        jdbc.update("DELETE FROM metricas_alertas_stock_diarias");
        jdbc.update("DELETE FROM alertas_stock");
    }

    @Test void invalidoSePersisteUnaVezSinCheckpoint() throws Exception {
        var checkpoint = mock(RecordProcessorCheckpointer.class);
        var detener = mock(Runnable.class);
        for (int i = 0; i < 2; i++) {
            var fatal = new AtomicBoolean();
            var p = new AnaliticasShardProcessor("stock", handler, cuarentena, new EstadoConsumidores.Stream(fatal), () -> false, detener, 100);
            p.initialize(InitializationInput.builder().shardId("shard").build());
            var record = KinesisClientRecord.builder().sequenceNumber("123").subSequenceNumber(2)
                    .data(ByteBuffer.wrap("{".getBytes(StandardCharsets.UTF_8))).build();
            p.processRecords(ProcessRecordsInput.builder().records(List.of(record)).checkpointer(checkpoint).build());
            assertThat(fatal).isTrue();
        }
        assertThat(jdbc.queryForObject("SELECT count(*) FROM eventos_cuarentena", Long.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT sub_sequence_number FROM eventos_cuarentena", Long.class)).isEqualTo(2);
        var lista = consultaCuarentena.listar("EVENTO_INVALIDO", 0, 25);
        assertThat(lista).hasSize(1);
        assertThat(lista.getFirst().subSequenceNumber()).isEqualTo(2);
        assertThat(consultaCuarentena.buscar(lista.getFirst().id())).isPresent();
        assertThat(consultaCuarentena.listar("COLISION_ID", 0, 25)).isEmpty();
        assertThat(consultaCuarentena.listar("' OR 1=1 --", 0, 25)).isEmpty();
        verify(checkpoint, never()).checkpoint();
        verify(detener, times(2)).run();
    }

    @Test void relecturaSimuladaTrasFalloDeCheckpointNoDuplicaMetricas() throws Exception {
        assertThat(consumers.getIfAvailable()).isNull();
        var checkpoint = mock(RecordProcessorCheckpointer.class);
        doThrow(new ThrottlingException("fallo")).when(checkpoint).checkpoint();
        var fatal = new AtomicBoolean();
        var detener = mock(Runnable.class);
        var primero = new AnaliticasShardProcessor("negocio", handler, cuarentena, new EstadoConsumidores.Stream(fatal), () -> false, detener, 100);
        primero.initialize(InitializationInput.builder().shardId("shard").build());
        primero.processRecords(input(checkpoint));
        assertThat(fatal).isTrue();
        verify(checkpoint, times(3)).checkpoint();
        verify(detener).run();
        var recuperado = mock(RecordProcessorCheckpointer.class);
        var segundo = new AnaliticasShardProcessor("negocio", handler, cuarentena, new EstadoConsumidores.Stream(), () -> false, () -> {}, 100);
        segundo.initialize(InitializationInput.builder().shardId("shard").build());
        segundo.processRecords(input(recuperado));
        verify(recuperado).checkpoint();
        assertThat(jdbc.queryForObject("SELECT ventas FROM metricas_ventas_diarias", Long.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM eventos_procesados", Long.class)).isEqualTo(1);
    }

    private ProcessRecordsInput input(RecordProcessorCheckpointer checkpoint) {
        byte[] evento = """
                {"eventId":"venta-kcl","eventType":"VentaConfirmada","schemaVersion":1,"occurredAt":"2026-11-12T22:00:00Z",
                 "payload":{"ventaId":1,"total":2.00,"items":[{"productoId":1,"sku":"S","cantidad":2,"precioUnitario":1.00,"subtotal":2.00}]}}
                """.getBytes(StandardCharsets.UTF_8);
        var record = KinesisClientRecord.builder().sequenceNumber("123").data(ByteBuffer.wrap(evento)).build();
        return ProcessRecordsInput.builder().records(List.of(record)).checkpointer(checkpoint).build();
    }
}
