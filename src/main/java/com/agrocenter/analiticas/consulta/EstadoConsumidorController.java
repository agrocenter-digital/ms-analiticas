package com.agrocenter.analiticas.consulta;

import com.agrocenter.analiticas.kinesis.AnaliticasKinesisConsumers;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Status;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Señal de esta instancia, no del estado global del stream. */
@RestController
@RequestMapping("/api/analiticas")
public class EstadoConsumidorController {
    private final boolean habilitado;
    private final ObjectProvider<AnaliticasKinesisConsumers> consumidor;

    public EstadoConsumidorController(@Value("${agrocenter.analiticas.kinesis.enabled:false}") boolean habilitado,
                                      ObjectProvider<AnaliticasKinesisConsumers> consumidor) {
        this.habilitado = habilitado;
        this.consumidor = consumidor;
    }

    @GetMapping("/estado-consumidor")
    public Estado estado() {
        if (!habilitado) return new Estado("DESACTIVADO", java.util.List.of());
        var actual = consumidor.getIfAvailable();
        if (actual == null) return new Estado("DESCONOCIDO", java.util.List.of());
        return new Estado(Status.UP.equals(actual.health().getStatus()) ? "LECTURAS_RECIENTES" : "NO_SALUDABLE", actual.lecturas());
    }

    public record Estado(String estado, java.util.List<AnaliticasKinesisConsumers.LecturaShard> lecturas) { }
}
