package com.agrocenter.analiticas.kinesis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.agrocenter.analiticas.eventos.AnaliticasProcessor;
import com.agrocenter.analiticas.eventos.CuarentenaService;
import com.agrocenter.analiticas.eventos.EventoInvalidoException;
import com.agrocenter.analiticas.eventos.ColisionEventoException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

class AnaliticasShardProcessorTest {
    @Test void confirmaSoloDespuesDeProcesarTodoElLote() throws Exception {
        var handler = mock(AnaliticasProcessor.class);
        var checkpoint = mock(RecordProcessorCheckpointer.class);
        processor(handler, new AtomicBoolean(), new AtomicBoolean(), mock(Runnable.class)).processRecords(input(checkpoint));
        var orden = inOrder(handler, checkpoint);
        orden.verify(handler, times(2)).procesar(any());
        orden.verify(checkpoint).checkpoint();
    }

    @Test void falloDeProcesamientoDuranteCierreNoConfirma() throws Exception {
        var handler = mock(AnaliticasProcessor.class);
        var stopping = new AtomicBoolean();
        var checkpoint = mock(RecordProcessorCheckpointer.class);
        when(handler.procesar(any())).thenAnswer(call -> { stopping.set(true); throw new IllegalStateException("fallo"); });
        processor(handler, new AtomicBoolean(), stopping, mock(Runnable.class)).processRecords(input(checkpoint));
        verify(checkpoint, never()).checkpoint();
    }

    @Test void checkpointTemporalSeReintentaSinRepetirAgregacion() throws Exception {
        var handler = mock(AnaliticasProcessor.class);
        var checkpoint = mock(RecordProcessorCheckpointer.class);
        doThrow(new ThrottlingException("temporal")).doNothing().when(checkpoint).checkpoint();
        processor(handler, new AtomicBoolean(), new AtomicBoolean(), mock(Runnable.class)).processRecords(input(checkpoint));
        verify(handler, times(2)).procesar(any());
        verify(checkpoint, times(2)).checkpoint();
    }

    @Test void checkpointAgotadoDetieneYNoConfirmaFinDeShard() throws Exception {
        var handler = mock(AnaliticasProcessor.class);
        var checkpoint = mock(RecordProcessorCheckpointer.class);
        var detener = mock(Runnable.class);
        var fatal = new AtomicBoolean();
        doThrow(new ThrottlingException("fallo")).when(checkpoint).checkpoint();
        var processor = processor(handler, fatal, new AtomicBoolean(), detener);
        processor.processRecords(input(checkpoint));
        processor.shardEnded(ShardEndedInput.builder().checkpointer(checkpoint).build());
        assertThat(fatal).isTrue();
        verify(checkpoint, times(3)).checkpoint();
        verify(detener).run();
    }

    @Test void invalidoOColisionSeGuardanYDetienenSinCheckpoint() throws Exception {
        for (boolean colision : new boolean[] {false, true}) {
            var handler = mock(AnaliticasProcessor.class);
            var cuarentena = mock(CuarentenaService.class);
            var checkpoint = mock(RecordProcessorCheckpointer.class);
            var detener = mock(Runnable.class);
            var fatal = new AtomicBoolean();
            when(handler.procesar(any())).thenThrow(colision ? new ColisionEventoException() : new EventoInvalidoException("invalido"));
            var p = new AnaliticasShardProcessor("stream", handler, cuarentena, new EstadoConsumidores.Stream(fatal), () -> false, detener, 100);
            p.initialize(InitializationInput.builder().shardId("shard").build());
            p.processRecords(input(checkpoint));
            var orden = inOrder(cuarentena, detener);
            orden.verify(cuarentena).guardar(eq("stream"), eq("shard"), eq("1"), eq(0L), any(), eq(colision ? "COLISION_ID" : "EVENTO_INVALIDO"));
            orden.verify(detener).run();
            verify(checkpoint, never()).checkpoint();
            assertThat(fatal).isTrue();
        }
    }

    @Test void fallaDeCuarentenaNoConfirma() throws Exception {
        var handler = mock(AnaliticasProcessor.class);
        var cuarentena = mock(CuarentenaService.class);
        var checkpoint = mock(RecordProcessorCheckpointer.class);
        var stopping = new AtomicBoolean();
        when(handler.procesar(any())).thenThrow(new EventoInvalidoException("invalido"));
        doAnswer(c -> { stopping.set(true); throw new IllegalStateException("bd"); }).when(cuarentena)
                .guardar(anyString(), anyString(), anyString(), anyLong(), any(), anyString());
        var p = new AnaliticasShardProcessor("stream", handler, cuarentena, new EstadoConsumidores.Stream(), stopping::get, mock(Runnable.class), 100);
        p.initialize(InitializationInput.builder().shardId("shard").build());
        p.processRecords(input(checkpoint));
        verify(checkpoint, never()).checkpoint();
    }

    private AnaliticasShardProcessor processor(AnaliticasProcessor handler, AtomicBoolean fatal,
                                                AtomicBoolean stopping, Runnable detener) {
        var processor = new AnaliticasShardProcessor("stream", handler, mock(CuarentenaService.class), new EstadoConsumidores.Stream(fatal), stopping::get, detener, 100);
        processor.initialize(InitializationInput.builder().shardId("shard").build());
        return processor;
    }

    private ProcessRecordsInput input(RecordProcessorCheckpointer checkpoint) {
        var a = KinesisClientRecord.builder().sequenceNumber("1").data(ByteBuffer.wrap(new byte[] {1})).build();
        var b = KinesisClientRecord.builder().sequenceNumber("2").data(ByteBuffer.wrap(new byte[] {2})).build();
        return ProcessRecordsInput.builder().records(List.of(a, b)).checkpointer(checkpoint).build();
    }
}
