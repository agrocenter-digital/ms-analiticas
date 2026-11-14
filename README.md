# ms-analiticas — implementación local parcial

La aplicación incorpora dos consumidores KCL preparados localmente, **desactivados por defecto** (`KINESIS_CONSUMER_ENABLED=false`), y una API local de solo lectura. **No está lista para desplegar**. La base final será `db_analiticas` en PostgreSQL de la EC2; las bases locales se usan solo para pruebas.

La API ofrece GET `/api/analiticas/resumen`, `/ventas`, `/productos`, `/b2b` y `/alertas-stock`. Exige access token Cognito con grupo `ADMIN`, issuer y cliente/audiencia configurados; `custom:role` no concede acceso. Requiere `COGNITO_ISSUER_URI`, `COGNITO_JWK_SET_URI`, `COGNITO_AUDIENCE`; el decoder se probó con tokens firmados y JWKS locales, pero falta probar tokens reales. Para consultas por fecha, `desde` es inclusivo y `hasta` exclusivo (fechas UTC), con rango máximo de dos años; por defecto se consultan los últimos 30 días UTC. Los listados aceptan `pagina` y `tamano` (máximo 100). La última alerta no representa stock actual. No hay métodos de escritura.

`GET /api/analiticas/cuarentena` y `GET /api/analiticas/cuarentena/{id}` permiten a ADMIN consultar únicamente metadatos diagnósticos. El listado admite `tipo` (`EVENTO_INVALIDO` o `COLISION_ID`) y paginación; no devuelve el payload ni permite resolver, descartar o reprocesar registros.

Los consumidores usan aplicaciones KCL independientes para negocio y stock, polling y `TRIM_HORIZON` solo cuando no hay checkpoint. Procesan transaccionalmente antes de confirmar el lote; errores transitorios se reintentan sin avanzar y un checkpoint temporal se reintenta hasta tres veces. Ante un evento inválido o conflicto de identidad se guarda el original en cuarentena y se detiene el consumidor sin confirmar el lote. No existe aún interfaz de revisión/replay; reiniciar sin resolverlo volverá a bloquearlo. El payload en Base64 **no está cifrado** y requiere política de acceso y retención. Si falla el checkpoint o se detecta un registro inválido, no hay reinicio automático. No activar hasta resolver estas brechas, verificar costos/permisos y probar KCL/DynamoDB reales.

Existe una operación interna de corrección, desactivada por defecto (`ANALYTICS_RESOLUTION_ENABLED=false`) y sin endpoint HTTP. Una herramienta administrativa separada y probada localmente usa un rol individual de BD y registra `current_user`; la corrección debe ser un evento relevante nuevo. Agregación y resolución se confirman en la misma transacción y se conserva el original. En una relectura solo se omite el registro si coinciden sus metadatos y bytes y la corrección continúa en eventos procesados. La detención de consumidores y aprobación siguen siendo comprobaciones humanas, y el rol operador puede actualizar métricas directamente: necesita controles adicionales. **No es aún un procedimiento desplegable**; falta validarlo con KCL real, permisos efectivos, identidad y carga. Ver [operacion/recuperacion-cuarentena.md](operacion/recuperacion-cuarentena.md).

`/actuator/health` muestra solo el estado general. Con KCL habilitado, el indicador verifica hilos, shards, reintentos, lecturas recientes y retraso; sus umbrales por defecto son dos minutos de silencio y una hora de retraso (`KINESIS_MAX_SILENCE_MS`, `KINESIS_MAX_LAG_MS`). Sin consumidores habilitados no existe indicador KCL: UP no implica consumo. Es señal de una instancia, no una alarma externa; su política de shards debe revisarse antes de escalar.

## Métricas implementadas

- Ventas confirmadas por día UTC: cantidad y monto exacto `NUMERIC`.
- Unidades y subtotal por día UTC, producto y SKU.
- Consultas B2B creadas y cambios de estado **como eventos ocurridos**, no como número actual de consultas por estado.
- Alertas de inventario crítico por día y última alerta por producto. **La última alerta no demuestra que el producto siga crítico**: aún no se procesan los eventos de recuperación/ajuste de stock. No presentar este dato como estado actual.

La marca de evento y los agregados comparten transacción. La identidad se deduplica por `eventId` y una huella SHA-256 de los bytes originales; el mismo ID con bytes diferentes se trata como conflicto, incluso si los JSON son semánticamente equivalentes. Los eventos tardíos se suman al día UTC de `occurredAt`. El desempate de alertas con la misma fecha usa `eventId`; no sustituye una versión monotónica de producto.

Validación local asume que el total coincide con la suma de subtotales y que cada subtotal es precio por cantidad; si el contrato incorpora descuentos/impuestos deberá versionarse antes de aceptarlo. Las métricas de stock actual, estado B2B actual y corrección de cancelaciones aún no están implementadas.

## Pruebas

El workflow `Verificacion manual ms-analiticas` se puede ejecutar manualmente desde GitHub Actions una vez creado el repositorio. Ejecuta las pruebas disponibles sin PostgreSQL externo y construye una imagen solo en el runner; no publica a Docker Hub ni despliega en EC2. Las pruebas que requieren PostgreSQL se omiten y deben validarse por separado. El workflow aún no se ha ejecutado en GitHub.

El workflow separado `Publicar imagen ms-analiticas (manual)` está preparado localmente para publicar solo desde `main`, con etiqueta `sha-<commit>` y digest en el resumen, previa configuración coordinada de `DOCKERHUB_USERNAME` y `DOCKERHUB_TOKEN`. No publica `latest` ni despliega en EC2. Una etiqueta de commit puede sobrescribirse al reconstruir; usar el digest registrado para identificar exactamente la imagen. No se ha ejecutado ni validado en GitHub y omite pruebas que requieren PostgreSQL.

`docker build -t ms-analiticas:local .` construye la imagen Java 21 sin usuario root. El healthcheck usa el puerto interno 8084; si se cambia `SERVER_PORT`, debe ajustarse también el healthcheck del despliegue. KCL está desactivado por defecto y `UP` no demuestra consumo. La configuración real de BD y Cognito sigue pendiente; no desplegar aún.

`mvn test` ejecuta pruebas unitarias y omite integración PostgreSQL salvo `ANALYTICS_PG_TEST=true`. La integración **borra las tablas de métricas y eventos procesados**: ejecutar únicamente en una base temporal dedicada.

```sh
ANALYTICS_PG_TEST=true DB_URL=jdbc:postgresql://localhost:5432/analiticas_test \
  DB_USERNAME=postgres DB_PASSWORD=localtest mvn verify
```
