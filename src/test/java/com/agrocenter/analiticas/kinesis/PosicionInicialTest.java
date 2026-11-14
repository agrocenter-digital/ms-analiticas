package com.agrocenter.analiticas.kinesis;

import static org.assertj.core.api.Assertions.*;
import org.junit.jupiter.api.Test;
import software.amazon.kinesis.common.InitialPositionInStream;
import com.agrocenter.analiticas.eventos.AnaliticasProcessor;
import com.agrocenter.analiticas.eventos.CuarentenaService;
import org.springframework.test.util.ReflectionTestUtils;
import static org.mockito.Mockito.mock;

class PosicionInicialTest {
    @Test void aceptaSoloPosicionesExplicitas() {
        assertThat(AnaliticasKinesisConsumers.validarPosicionInicial("TRIM_HORIZON")).isEqualTo(InitialPositionInStream.TRIM_HORIZON);
        assertThat(AnaliticasKinesisConsumers.validarPosicionInicial("LATEST")).isEqualTo(InitialPositionInStream.LATEST);
        assertThatThrownBy(() -> AnaliticasKinesisConsumers.validarPosicionInicial("AT_TIMESTAMP")).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void rechazaValorInvalidoAntesDeIniciarClientesAws() {
        var consumer = new AnaliticasKinesisConsumers(mock(AnaliticasProcessor.class), mock(CuarentenaService.class));
        ReflectionTestUtils.setField(consumer, "businessApplication", "negocio");
        ReflectionTestUtils.setField(consumer, "stockApplication", "stock");
        ReflectionTestUtils.setField(consumer, "initialPosition", "invalido");
        assertThatThrownBy(consumer::start).isInstanceOf(IllegalArgumentException.class);
    }
}
