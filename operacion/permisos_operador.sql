-- Solo después de migrar y únicamente con un administrador de la BD correspondiente.
-- Uso: psql ... -v operator_role=rol_individual -f permisos_operador.sql
-- No elimina permisos heredados o concedidos a PUBLIC; comprobar privilegios efectivos.
\set ON_ERROR_STOP on
\if :{?operator_role}
\else
    \echo 'Falta -v operator_role=rol_individual'
    \quit 3
\endif
GRANT USAGE ON SCHEMA public TO :"operator_role";
REVOKE ALL PRIVILEGES ON TABLE public.eventos_procesados, public.eventos_cuarentena,
    public.metricas_ventas_diarias, public.metricas_productos_diarias, public.metricas_b2b_diarias,
    public.metricas_alertas_stock_diarias, public.alertas_stock FROM :"operator_role";
GRANT SELECT, INSERT ON TABLE public.eventos_procesados TO :"operator_role";
GRANT SELECT, INSERT, UPDATE ON TABLE public.metricas_ventas_diarias, public.metricas_productos_diarias,
    public.metricas_b2b_diarias, public.metricas_alertas_stock_diarias, public.alertas_stock TO :"operator_role";
GRANT SELECT ON TABLE public.eventos_cuarentena TO :"operator_role";
GRANT UPDATE (resuelto_en, resuelto_por, motivo_resolucion, event_id_corregido)
    ON TABLE public.eventos_cuarentena TO :"operator_role";
