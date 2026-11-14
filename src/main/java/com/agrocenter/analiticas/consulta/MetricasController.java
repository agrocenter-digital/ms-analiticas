package com.agrocenter.analiticas.consulta;

import java.time.Clock;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/analiticas")
public class MetricasController {
    private final MetricasRepository repository;
    public MetricasController(MetricasRepository repository) { this.repository = repository; }

    @GetMapping("/resumen")
    public MetricasRepository.Resumen resumen(@RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate desde,
                                               @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate hasta) {
        var rango = rango(desde, hasta); return repository.resumen(rango.desde(), rango.hasta());
    }

    @GetMapping("/ventas")
    public Pagina<MetricasRepository.VentaDiaria> ventas(@RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate desde,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate hasta,
            @RequestParam(defaultValue = "0") int pagina, @RequestParam(defaultValue = "25") int tamano) {
        validarPagina(pagina, tamano); var r = rango(desde, hasta);
        return pagina(repository.ventas(r.desde(), r.hasta(), pagina, tamano), pagina, tamano);
    }

    @GetMapping("/productos")
    public Pagina<MetricasRepository.ProductoDiario> productos(@RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate desde,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate hasta,
            @RequestParam(defaultValue = "0") int pagina, @RequestParam(defaultValue = "25") int tamano) {
        validarPagina(pagina, tamano); var r = rango(desde, hasta);
        return pagina(repository.productos(r.desde(), r.hasta(), pagina, tamano), pagina, tamano);
    }

    @GetMapping("/b2b")
    public Pagina<MetricasRepository.B2bDiario> b2b(@RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate desde,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate hasta,
            @RequestParam(defaultValue = "0") int pagina, @RequestParam(defaultValue = "25") int tamano) {
        validarPagina(pagina, tamano); var r = rango(desde, hasta);
        return pagina(repository.b2b(r.desde(), r.hasta(), pagina, tamano), pagina, tamano);
    }

    @GetMapping("/alertas-stock")
    public Pagina<MetricasRepository.AlertaStock> alertas(@RequestParam(defaultValue = "0") int pagina,
                                                           @RequestParam(defaultValue = "25") int tamano) {
        validarPagina(pagina, tamano); return pagina(repository.alertas(pagina, tamano), pagina, tamano);
    }

    private Rango rango(LocalDate desde, LocalDate hasta) {
        LocalDate fin = hasta == null ? LocalDate.now(Clock.system(ZoneOffset.UTC)).plusDays(1) : hasta;
        LocalDate inicio = desde == null ? fin.minusDays(30) : desde;
        if (!inicio.isBefore(fin) || inicio.plusYears(2).isBefore(fin))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Rango invalido o mayor a dos años");
        return new Rango(inicio, fin);
    }
    private void validarPagina(int pagina, int tamano) {
        if (pagina < 0 || pagina > 10000 || tamano < 1 || tamano > 100)
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Paginacion invalida");
    }
    private <T> Pagina<T> pagina(List<T> elementos, int pagina, int tamano) {
        return new Pagina<>(List.copyOf(elementos.subList(0, Math.min(tamano, elementos.size()))), pagina, tamano, elementos.size() > tamano);
    }
    private record Rango(LocalDate desde, LocalDate hasta) { }
    public record Pagina<T>(List<T> elementos, int pagina, int tamano, boolean hayMas) { }
}
