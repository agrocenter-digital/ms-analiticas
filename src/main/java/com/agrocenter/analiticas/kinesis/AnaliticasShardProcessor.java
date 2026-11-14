package com.agrocenter.analiticas.kinesis;

import com.agrocenter.analiticas.eventos.AnaliticasProcessor;
import com.agrocenter.analiticas.eventos.CuarentenaService;
import com.agrocenter.analiticas.eventos.EventoInvalidoException;
import com.agrocenter.analiticas.eventos.ColisionEventoException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.kinesis.exceptions.KinesisClientLibRetryableException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;

/** No confirma un lote hasta que todas las transacciones de agregación hayan terminado. */
final class AnaliticasShardProcessor implements ShardRecordProcessor {
    private static final Logger log = LoggerFactory.getLogger(AnaliticasShardProcessor.class);
    private final String stream;
    private final AnaliticasProcessor processor;
    private final CuarentenaService cuarentena;
    private final EstadoConsumidores.Stream estadoStream;
    private final EstadoConsumidores.Shard estadoShard = new EstadoConsumidores.Shard();
    private final AtomicBoolean fatal;
    private final BooleanSupplier stopping;
    private final Runnable detener;
    private final long retryDelayMs;
    private String shard;

    AnaliticasShardProcessor(String stream, AnaliticasProcessor processor, CuarentenaService cuarentena, EstadoConsumidores.Stream estadoStream,
                            BooleanSupplier stopping, Runnable detener, long retryDelayMs) {
        this.stream = stream; this.processor = processor; this.cuarentena = cuarentena;
        this.estadoStream = estadoStream; this.fatal = estadoStream.fatal;
        this.stopping = stopping; this.detener = detener; this.retryDelayMs = retryDelayMs;
    }

    @Override public void initialize(InitializationInput input) { shard = input.shardId(); estadoStream.shards.put(shard, estadoShard); }
    @Override public void processRecords(ProcessRecordsInput input) {
        if (fatal.get() || stopping.getAsBoolean()) return;
        try {
            for (var record : input.records()) {
                var buffer = record.data().asReadOnlyBuffer();
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                while (!fatal.get() && !stopping.getAsBoolean()) {
                    try {
                        if (!cuarentena.resuelta(stream, shard, record.sequenceNumber(), record.subSequenceNumber(), bytes))
                            processor.procesar(bytes);
                        estadoShard.retrying = false;
                        break;
                    }
                    catch (EventoInvalidoException | ColisionEventoException e) {
                        estadoShard.retrying = true;
                        try {
                            cuarentena.guardar(stream, shard, record.sequenceNumber(), record.subSequenceNumber(), bytes,
                                    e instanceof ColisionEventoException ? "COLISION_ID" : "EVENTO_INVALIDO");
                        } catch (RuntimeException fallo) {
                            log.error("No se pudo guardar cuarentena stream={} shard={} tipo={}; reintentando", stream, shard, fallo.getClass().getSimpleName());
                            Thread.sleep(Math.max(100, retryDelayMs));
                            continue;
                        }
                        fallar("registro en cuarentena; intervencion necesaria");
                        return;
                    }
                    catch (RuntimeException e) {
                        estadoShard.retrying = true;
                        log.warn("Fallo al procesar evento stream={} shard={} sequence={} tipo={}; reintentando sin checkpoint",
                                stream, shard, record.sequenceNumber(), e.getClass().getSimpleName());
                        Thread.sleep(Math.max(100, retryDelayMs));
                    }
                }
                if (fatal.get() || stopping.getAsBoolean()) return;
            }
            if (!input.records().isEmpty() && !confirmar(input.checkpointer())) return;
            estadoShard.lagMs = input.millisBehindLatest();
            estadoShard.observedAt = System.nanoTime();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fallar("interrumpido");
        } catch (Exception e) { fallar(e.getClass().getSimpleName()); }
    }

    private boolean confirmar(RecordProcessorCheckpointer checkpointer) throws Exception {
        for (int intento = 1; intento <= 3 && !fatal.get() && !stopping.getAsBoolean(); intento++) {
            try { checkpointer.checkpoint(); estadoShard.retrying = false; return true; }
            catch (KinesisClientLibRetryableException e) {
                estadoShard.retrying = true;
                if (intento == 3) throw e;
                Thread.sleep(200L * intento);
            }
        }
        return false;
    }

    private void fallar(String tipo) {
        fatal.set(true);
        log.error("Consumidor analiticas detenido stream={} shard={} error={}", stream, shard, tipo);
        detener.run();
    }
    @Override public void leaseLost(LeaseLostInput input) { if (shard != null) estadoStream.shards.remove(shard, estadoShard); log.warn("Lease perdido stream={} shard={}", stream, shard); }
    @Override public void shardEnded(ShardEndedInput input) {
        if (fatal.get() || stopping.getAsBoolean()) return;
        try { if (confirmar(input.checkpointer()) && shard != null) estadoStream.shards.remove(shard, estadoShard); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); fallar("interrumpido"); }
        catch (Exception e) { fallar(e.getClass().getSimpleName()); }
    }
    @Override public void shutdownRequested(ShutdownRequestedInput input) { /* No avanzar sin procesar. */ }
}
