package com.agrocenter.analiticas.eventos;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.HexFormat;
import java.util.Set;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class AnaliticasProcessor {
    private static final Set<String> TIPOS = Set.of("VentaConfirmada", "ConsultaB2BCreada", "ConsultaB2BEstadoActualizado", "InventarioCritico");
    private static final Set<String> ESTADOS = Set.of("PENDIENTE", "RESPONDIDO", "ARCHIVADO");
    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    public AnaliticasProcessor(JdbcTemplate jdbc, ObjectMapper mapper) { this.jdbc = jdbc; this.mapper = mapper; }

    /** true: evento nuevo agregado; false: duplicado idéntico o tipo no relevante. */
    @Transactional
    public boolean procesar(byte[] bytes) {
        if (bytes == null) throw new EventoInvalidoException("Evento ausente");
        try {
            JsonNode n = mapper.readTree(bytes);
            if (n == null || !n.isObject()) throw new EventoInvalidoException("Sobre invalido");
            String tipo = texto(n, "eventType", 100);
            if (!TIPOS.contains(tipo)) return false;
            String id = texto(n, "eventId", 100);
            if (!n.path("schemaVersion").isIntegralNumber() || !n.path("schemaVersion").canConvertToInt()
                    || n.path("schemaVersion").intValue() != 1 || !n.path("payload").isObject())
                throw new EventoInvalidoException("Version o payload invalido");
            Instant fechaEvento = Instant.parse(texto(n, "occurredAt", 100));
            var fecha = java.sql.Date.valueOf(fechaEvento.atZone(ZoneOffset.UTC).toLocalDate());
            JsonNode payload = n.path("payload");
            if ("VentaConfirmada".equals(tipo)) validarVenta(payload);
            else if ("InventarioCritico".equals(tipo)) validarStock(payload);
            else validarB2b(payload, tipo);
            String huella = huella(bytes);
            int insertado = jdbc.update("INSERT INTO eventos_procesados(event_id,tipo,huella,procesado_en) VALUES (?,?,?,?) ON CONFLICT (event_id) DO NOTHING",
                    id, tipo, huella, java.sql.Timestamp.from(Instant.now()));
            if (insertado == 0) {
                String anterior = jdbc.queryForObject("SELECT huella FROM eventos_procesados WHERE event_id=?", String.class, id);
                if (!huella.equals(anterior)) throw new ColisionEventoException();
                return false;
            }
            if ("VentaConfirmada".equals(tipo)) {
                BigDecimal total = dinero(payload, "total");
                jdbc.update("""
                        INSERT INTO metricas_ventas_diarias(fecha, ventas, monto) VALUES (?, 1, ?)
                        ON CONFLICT(fecha) DO UPDATE SET ventas=metricas_ventas_diarias.ventas+1, monto=metricas_ventas_diarias.monto+EXCLUDED.monto
                        """, fecha, total);
                for (JsonNode item : payload.path("items")) {
                    jdbc.update("""
                            INSERT INTO metricas_productos_diarias(fecha, producto_id, sku, unidades, monto) VALUES (?, ?, ?, ?, ?)
                            ON CONFLICT(fecha, producto_id, sku) DO UPDATE SET unidades=metricas_productos_diarias.unidades+EXCLUDED.unidades, monto=metricas_productos_diarias.monto+EXCLUDED.monto
                            """, fecha, item.path("productoId").longValue(), texto(item, "sku", 100), item.path("cantidad").longValue(), dinero(item, "subtotal"));
                }
            } else if ("InventarioCritico".equals(tipo)) {
                jdbc.update("""
                        INSERT INTO metricas_alertas_stock_diarias(fecha, alertas) VALUES (?, 1)
                        ON CONFLICT(fecha) DO UPDATE SET alertas=metricas_alertas_stock_diarias.alertas+1
                        """, fecha);
                jdbc.update("""
                        INSERT INTO alertas_stock(producto_id, sku, stock_actual, stock_minimo, ultimo_evento, event_id)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(producto_id) DO UPDATE SET sku=EXCLUDED.sku, stock_actual=EXCLUDED.stock_actual,
                          stock_minimo=EXCLUDED.stock_minimo, ultimo_evento=EXCLUDED.ultimo_evento, event_id=EXCLUDED.event_id
                        WHERE (alertas_stock.ultimo_evento, alertas_stock.event_id) < (EXCLUDED.ultimo_evento, EXCLUDED.event_id)
                        """, payload.path("productoId").longValue(), texto(payload, "sku", 100),
                        payload.path("stockActual").intValue(), payload.path("stockMinimo").intValue(),
                        java.sql.Timestamp.from(fechaEvento), id);
            } else {
                int creadas = "ConsultaB2BCreada".equals(tipo) ? 1 : 0;
                jdbc.update("""
                        INSERT INTO metricas_b2b_diarias(fecha, consultas_creadas, cambios_estado) VALUES (?, ?, ?)
                        ON CONFLICT(fecha) DO UPDATE SET consultas_creadas=metricas_b2b_diarias.consultas_creadas+EXCLUDED.consultas_creadas, cambios_estado=metricas_b2b_diarias.cambios_estado+EXCLUDED.cambios_estado
                        """, fecha, creadas, 1 - creadas);
            }
            return true;
        } catch (IOException | DateTimeParseException e) { throw new EventoInvalidoException("JSON o fecha invalida", e); }
    }

    private static void validarVenta(JsonNode payload) {
        enteroPositivo(payload, "ventaId");
        BigDecimal total = dinero(payload, "total");
        JsonNode items = payload.path("items");
        if (!items.isArray() || items.isEmpty()) throw new EventoInvalidoException("Venta sin items");
        BigDecimal suma = BigDecimal.ZERO;
        for (JsonNode item : items) {
            enteroPositivo(item, "productoId"); enteroPositivo(item, "cantidad"); texto(item, "sku", 100);
            BigDecimal subtotal = dinero(item, "subtotal");
            BigDecimal precio = dinero(item, "precioUnitario");
            if (precio.multiply(BigDecimal.valueOf(item.path("cantidad").longValue())).compareTo(subtotal) != 0)
                throw new EventoInvalidoException("Subtotal inconsistente");
            suma = suma.add(subtotal);
        }
        if (suma.compareTo(total) != 0) throw new EventoInvalidoException("Total inconsistente");
    }

    private static void validarB2b(JsonNode payload, String tipo) {
        enteroPositivo(payload, "consultaId");
        String estado = texto(payload, "estadoActual", 30);
        if (!ESTADOS.contains(estado)) throw new EventoInvalidoException("Estado B2B invalido");
        if ("ConsultaB2BCreada".equals(tipo) && !"PENDIENTE".equals(estado))
            throw new EventoInvalidoException("Estado inicial B2B invalido");
        if ("ConsultaB2BEstadoActualizado".equals(tipo)) {
            String anterior = texto(payload, "estadoAnterior", 30);
            if (!ESTADOS.contains(anterior) || anterior.equals(estado)) throw new EventoInvalidoException("Transicion B2B invalida");
        }
    }

    private static void validarStock(JsonNode payload) {
        enteroPositivo(payload, "productoId");
        texto(payload, "sku", 100);
        for (String campo : new String[] {"stockActual", "stockMinimo"}) {
            var valor = payload.path(campo);
            if (!valor.isIntegralNumber() || !valor.canConvertToInt() || valor.intValue() < 0)
                throw new EventoInvalidoException("Stock invalido: " + campo);
        }
        if (payload.path("stockActual").intValue() > payload.path("stockMinimo").intValue())
            throw new EventoInvalidoException("Alerta de stock no critico");
    }

    private static long enteroPositivo(JsonNode node, String campo) {
        JsonNode valor = node.path(campo);
        if (!valor.isIntegralNumber() || !valor.canConvertToLong() || valor.longValue() <= 0)
            throw new EventoInvalidoException("Campo numerico invalido: " + campo);
        return valor.longValue();
    }

    private static BigDecimal dinero(JsonNode node, String campo) {
        JsonNode valor = node.path(campo);
        if (!valor.isNumber()) throw new EventoInvalidoException("Monto invalido: " + campo);
        try {
            BigDecimal monto = valor.decimalValue().setScale(2, RoundingMode.UNNECESSARY);
            if (monto.signum() < 0 || monto.precision() > 20) throw new EventoInvalidoException("Monto fuera de rango: " + campo);
            return monto;
        } catch (ArithmeticException e) { throw new EventoInvalidoException("Monto invalido: " + campo, e); }
    }

    private static String texto(JsonNode node, String campo, int max) {
        JsonNode valor = node.path(campo);
        if (!valor.isTextual() || valor.textValue().isBlank() || valor.textValue().length() > max)
            throw new EventoInvalidoException("Campo invalido: " + campo);
        return valor.textValue();
    }

    private static String huella(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }
}
