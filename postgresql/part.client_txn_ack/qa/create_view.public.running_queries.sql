-- View: running_queries

-- DROP VIEW running_queries;

CREATE OR REPLACE VIEW public.running_queries AS 
 SELECT now() - pg_stat_activity.query_start AS running_time, regexp_replace(pg_stat_activity.query, '[[:space:]]+'::text, ' '::text, 'g'::text) AS "-----------------------User Query------------------------------", pg_stat_activity.datname, pg_stat_activity.usename, pg_stat_activity.client_addr AS "Client Address", pg_stat_activity.client_port, pg_stat_activity.pid, pg_stat_activity.usesysid, pg_stat_activity.datid, pg_stat_activity.waiting, pg_stat_activity.xact_start, pg_stat_activity.query_start, pg_stat_activity.backend_start
   FROM pg_stat_activity
  WHERE pg_stat_activity.query <> '<IDLE>'::text
  ORDER BY now() - pg_stat_activity.query_start DESC;

ALTER TABLE public.running_queries
  OWNER TO postgres;


