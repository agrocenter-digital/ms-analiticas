package com.agrocenter.analiticas.kinesis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import com.agrocenter.analiticas.eventos.AnaliticasProcessor;
import com.agrocenter.analiticas.eventos.CuarentenaService;
import java.util.List;
import org.junit.jupiter.api.Test;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;

class EstadoConsumidoresTest {
    @Test void exigeAmbosStreamsYDetectaRetrasoSilencioYFallo() {
        var estados = new EstadoConsumidores();
        assertThat(estados.salud(1000, 120000).getStatus().getCode()).isEqualTo("DOWN");
        var negocio = estados.registrar("negocio"); var stock = estados.registrar("stock");
        negocio.worker = Thread.currentThread(); stock.worker = Thread.currentThread();
        var p1 = new AnaliticasShardProcessor("negocio", mock(AnaliticasProcessor.class), mock(CuarentenaService.class), negocio, () -> false, () -> {}, 100);
        var p2 = new AnaliticasShardProcessor("stock", mock(AnaliticasProcessor.class), mock(CuarentenaService.class), stock, () -> false, () -> {}, 100);
        p1.initialize(InitializationInput.builder().shardId("n").build());
        p2.initialize(InitializationInput.builder().shardId("s").build());
        p1.processRecords(ProcessRecordsInput.builder().records(List.of()).millisBehindLatest(0L).build());
        assertThat(estados.salud(1000, 120000).getStatus().getCode()).isEqualTo("DOWN");
        p2.processRecords(ProcessRecordsInput.builder().records(List.of()).millisBehindLatest(0L).build());
        assertThat(estados.salud(1000, 120000).getStatus().getCode()).isEqualTo("UP");
        p2.processRecords(ProcessRecordsInput.builder().records(List.of()).millisBehindLatest(1001L).build());
        assertThat(estados.salud(1000, 120000).getStatus().getCode()).isEqualTo("DOWN");
        p2.processRecords(ProcessRecordsInput.builder().records(List.of()).millisBehindLatest(0L).build());
        assertThat(estados.salud(1000, -1).getStatus().getCode()).isEqualTo("DOWN");
        p2.leaseLost(LeaseLostInput.builder().build());
        assertThat(estados.salud(1000, 120000).getStatus().getCode()).isEqualTo("DOWN");
        negocio.fatal.set(true);
        assertThat(estados.salud(1000, 120000).getStatus().getCode()).isEqualTo("DOWN");
    }
}
