package com.agrocenter.analiticas.eventos;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Operación interna experimental; no tiene endpoint ni autenticación de operador. */
@Service
@ConditionalOnProperty(name = "agrocenter.analiticas.resolucion.enabled", havingValue = "true")
public class ResolucionCuarentenaService {
    private final JdbcTemplate jdbc;
    private final AnaliticasProcessor processor;
    private final ObjectMapper mapper;
    public ResolucionCuarentenaService(JdbcTemplate jdbc, AnaliticasProcessor processor, ObjectMapper mapper) {
        this.jdbc = jdbc; this.processor = processor; this.mapper = mapper;
    }

    @Transactional
    public void resolver(String id, String operador, String motivo, byte[] corregido) {
        if (id == null || !id.matches("[0-9a-f]{64}") || operador == null || operador.isBlank() || operador.length() > 200
                || motivo == null || motivo.isBlank() || motivo.length() > 500 || corregido == null || corregido.length == 0 || corregido.length > 1024 * 1024)
            throw new IllegalArgumentException("Datos de resolucion invalidos");
        List<Boolean> registros = jdbc.query("SELECT resuelto_en IS NOT NULL FROM eventos_cuarentena WHERE id = ? FOR UPDATE",
                (rs, n) -> rs.getBoolean(1), id);
        if (registros.isEmpty()) throw new IllegalArgumentException("Registro de cuarentena inexistente");
        if (registros.getFirst()) throw new IllegalStateException("Registro de cuarentena ya resuelto");
        if (!processor.procesar(corregido)) throw new IllegalStateException("Correccion irrelevante o ya procesada");
        try {
            String eventId = mapper.readTree(corregido).path("eventId").textValue();
            int actualizadas = jdbc.update("""
                    UPDATE eventos_cuarentena SET resuelto_en = ?, resuelto_por = ?, motivo_resolucion = ?, event_id_corregido = ?
                    WHERE id = ? AND resuelto_en IS NULL
                    """, Timestamp.from(Instant.now()), operador, motivo, eventId, id);
            if (actualizadas != 1) throw new IllegalStateException("No se pudo registrar la resolucion");
        } catch (java.io.IOException e) { throw new EventoInvalidoException("Evento corregido invalido", e); }
    }
}
