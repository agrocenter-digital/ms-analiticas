package com.agrocenter.analiticas.eventos;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

class AnaliticasProcessorTest {
    private final JdbcTemplate jdbc = mock(JdbcTemplate.class);
    private final AnaliticasProcessor processor = new AnaliticasProcessor(jdbc, new ObjectMapper());

    @Test void omiteTiposNoRelevantesYRechazaSobreInvalido() {
        assertThat(processor.procesar("{\"eventType\":\"CompraCreada\"}".getBytes(StandardCharsets.UTF_8))).isFalse();
        assertThatThrownBy(() -> processor.procesar("{}".getBytes(StandardCharsets.UTF_8))).isInstanceOf(EventoInvalidoException.class);
        assertThatThrownBy(() -> processor.procesar("{".getBytes(StandardCharsets.UTF_8))).isInstanceOf(EventoInvalidoException.class);
        verifyNoInteractions(jdbc);
    }

    @Test void ventaInconsistenteNoGeneraEscrituras() {
        byte[] evento = """
                {"eventId":"v1","eventType":"VentaConfirmada","schemaVersion":1,"occurredAt":"2026-11-12T22:00:00Z",
                 "payload":{"ventaId":1,"total":3,"items":[{"productoId":1,"sku":"S","cantidad":2,"precioUnitario":1,"subtotal":2}]}}
                """.getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(() -> processor.procesar(evento)).isInstanceOf(EventoInvalidoException.class);
        verifyNoInteractions(jdbc);
    }
}
