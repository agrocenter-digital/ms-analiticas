CREATE TABLE eventos_procesados (
    event_id VARCHAR(100) PRIMARY KEY,
    tipo VARCHAR(100) NOT NULL,
    huella VARCHAR(64) NOT NULL,
    procesado_en TIMESTAMP WITH TIME ZONE NOT NULL
);
CREATE TABLE metricas_ventas_diarias (
    fecha DATE PRIMARY KEY,
    ventas BIGINT NOT NULL,
    monto NUMERIC(20,2) NOT NULL
);
CREATE TABLE metricas_productos_diarias (
    fecha DATE NOT NULL,
    producto_id BIGINT NOT NULL,
    sku VARCHAR(100) NOT NULL,
    unidades BIGINT NOT NULL,
    monto NUMERIC(20,2) NOT NULL,
    PRIMARY KEY (fecha, producto_id, sku)
);
CREATE TABLE metricas_b2b_diarias (
    fecha DATE PRIMARY KEY,
    consultas_creadas BIGINT NOT NULL,
    cambios_estado BIGINT NOT NULL
);
CREATE TABLE alertas_stock (
    producto_id BIGINT PRIMARY KEY,
    sku VARCHAR(100) NOT NULL,
    stock_actual INTEGER NOT NULL,
    stock_minimo INTEGER NOT NULL,
    ultimo_evento TIMESTAMP WITH TIME ZONE NOT NULL,
    event_id VARCHAR(100) NOT NULL
);
CREATE TABLE metricas_alertas_stock_diarias (
    fecha DATE PRIMARY KEY,
    alertas BIGINT NOT NULL
);
