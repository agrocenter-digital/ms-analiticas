# Recuperación de analíticas — preparación local, no autorizada aún en EC2

La herramienta corrige únicamente métricas de analíticas. No modifica ventas, compras,
inventario ni auditoría, y no republica el evento original.

1. Identificar el incidente y detener **todas** las instancias consumidoras de analíticas.
   La confirmación del comando es una declaración del operador, no una verificación automática.
2. Inspeccionar el original mediante acceso a BD restringido y preparar un JSON corregido
   con identidad y fecha apropiadas. Verificar el contrato, las métricas resultantes y
   que no se trate de una venta ya contada bajo otro `eventId`. Revisar y aprobar la
   corrección antes de ejecutarla; la herramienta no detecta duplicados de negocio con IDs distintos.
3. Aplicar migraciones por separado y preparar un rol operador individual con
   `operacion/permisos_operador.sql`. Debe ser distinto del consumidor, no propietario y
   no tener privilegios elevados ni permisos heredados excesivos. Verificar permisos de PUBLIC.
   Este rol necesita UPDATE de métricas para aplicar la corrección; puede modificarlas fuera
   de la herramienta, por lo que requiere control de acceso y registro operativo independientes.
4. Empaquetar y generar classpath en el proyecto local:

   ```sh
   mvn -DskipTests package dependency:build-classpath -Dmdep.outputFile=target/operator-classpath.txt
   ```

5. Configurar privadamente `DB_URL`, `DB_USERNAME`, `DB_PASSWORD` y
   `ANALYTICS_CONSUMER_DB_USERNAME`. `DB_URL` debe apuntar a la base correcta;
   no incluir contraseñas en argumentos ni en Git.

   ```sh
   java -cp "target/classes:$(cat target/operator-classpath.txt)" \
     com.agrocenter.analiticas.operacion.ResolucionCommand \
     --confirmo-consumidores-detenidos ID_CUARENTENA /ruta/privada/corregido.json \
     'Motivo revisado sin datos sensibles'
   ```

   El comando no inicia KCL, HTTP ni Flyway. Guarda `db:current_user`, motivo y una
   corrección nueva en una transacción. El motivo puede ser visible en la lista de procesos;
   no incluir datos sensibles. Una cuenta compartida no identifica a una persona.
6. Verificar resolución, métricas y trazabilidad antes de reiniciar consumidores.
   No editar checkpoints manualmente. La relectura solo salta el registro original exacto
   después de que su corrección se haya confirmado.

No se ha probado este procedimiento con Kinesis real. Si la retención de 24 horas se
agotó, una corrección no recupera automáticamente eventos intermedios: requiere
reconciliación con S3/outbox y un procedimiento adicional. No habilitar en producción
hasta validar el aislamiento, los permisos efectivos, la identidad de operador y KCL.
