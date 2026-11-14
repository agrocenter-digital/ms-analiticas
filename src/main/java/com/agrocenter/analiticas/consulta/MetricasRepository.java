package com.agrocenter.analiticas.consulta;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class MetricasRepository {
    private final JdbcTemplate jdbc;
    public MetricasRepository(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    public Resumen resumen(LocalDate desde, LocalDate hasta) {
        return jdbc.queryForObject("""
                SELECT COALESCE((SELECT SUM(ventas) FROM metricas_ventas_diarias WHERE fecha >= ? AND fecha < ?),0),
                       COALESCE((SELECT SUM(monto) FROM metricas_ventas_diarias WHERE fecha >= ? AND fecha < ?),0),
                       COALESCE((SELECT SUM(consultas_creadas) FROM metricas_b2b_diarias WHERE fecha >= ? AND fecha < ?),0),
                       COALESCE((SELECT SUM(cambios_estado) FROM metricas_b2b_diarias WHERE fecha >= ? AND fecha < ?),0),
                       COALESCE((SELECT SUM(alertas) FROM metricas_alertas_stock_diarias WHERE fecha >= ? AND fecha < ?),0)
                """, (rs, n) -> new Resumen(desde, hasta, "UTC", rs.getLong(1), rs.getBigDecimal(2), rs.getLong(3), rs.getLong(4), rs.getLong(5)),
                Date.valueOf(desde), Date.valueOf(hasta), Date.valueOf(desde), Date.valueOf(hasta), Date.valueOf(desde), Date.valueOf(hasta),
                Date.valueOf(desde), Date.valueOf(hasta), Date.valueOf(desde), Date.valueOf(hasta));
    }

    public List<VentaDiaria> ventas(LocalDate desde, LocalDate hasta, int pagina, int tamano) {
        return jdbc.query("SELECT fecha, ventas, monto FROM metricas_ventas_diarias WHERE fecha >= ? AND fecha < ? ORDER BY fecha DESC LIMIT ? OFFSET ?",
                (rs, n) -> new VentaDiaria(rs.getDate(1).toLocalDate(), rs.getLong(2), rs.getBigDecimal(3)),
                Date.valueOf(desde), Date.valueOf(hasta), tamano + 1, (long) pagina * tamano);
    }

    public List<ProductoDiario> productos(LocalDate desde, LocalDate hasta, int pagina, int tamano) {
        return jdbc.query("""
                SELECT fecha, producto_id, sku, unidades, monto FROM metricas_productos_diarias
                WHERE fecha >= ? AND fecha < ? ORDER BY fecha DESC, producto_id ASC, sku ASC LIMIT ? OFFSET ?
                """, (rs, n) -> new ProductoDiario(rs.getDate(1).toLocalDate(), rs.getLong(2), rs.getString(3), rs.getLong(4), rs.getBigDecimal(5)),
                Date.valueOf(desde), Date.valueOf(hasta), tamano + 1, (long) pagina * tamano);
    }

    public List<B2bDiario> b2b(LocalDate desde, LocalDate hasta, int pagina, int tamano) {
        return jdbc.query("SELECT fecha, consultas_creadas, cambios_estado FROM metricas_b2b_diarias WHERE fecha >= ? AND fecha < ? ORDER BY fecha DESC LIMIT ? OFFSET ?",
                (rs, n) -> new B2bDiario(rs.getDate(1).toLocalDate(), rs.getLong(2), rs.getLong(3)),
                Date.valueOf(desde), Date.valueOf(hasta), tamano + 1, (long) pagina * tamano);
    }

    public List<AlertaStock> alertas(int pagina, int tamano) {
        return jdbc.query("SELECT producto_id, sku, stock_actual, stock_minimo, ultimo_evento, event_id FROM alertas_stock ORDER BY ultimo_evento DESC, producto_id ASC LIMIT ? OFFSET ?",
                (rs, n) -> new AlertaStock(rs.getLong(1), rs.getString(2), rs.getInt(3), rs.getInt(4), rs.getTimestamp(5).toInstant(), rs.getString(6)),
                tamano + 1, (long) pagina * tamano);
    }

    public record Resumen(LocalDate desde, LocalDate hastaExclusivo, String zona, long ventas, BigDecimal monto,
                          long consultasCreadas, long cambiosEstado, long alertasEmitidas) { }
    public record VentaDiaria(LocalDate fecha, long ventas, BigDecimal monto) { }
    public record ProductoDiario(LocalDate fecha, long productoId, String sku, long unidades, BigDecimal monto) { }
    public record B2bDiario(LocalDate fecha, long consultasCreadas, long cambiosEstado) { }
    public record AlertaStock(long productoId, String sku, int stockAlDetectar, int minimoAlDetectar, Instant ocurridoEn, String eventId) { }
}
