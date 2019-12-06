-- Function: public.mk_grants_to_schema_roles_by_owner(text)

-- DROP FUNCTION public.mk_grants_to_schema_roles_by_owner(text);

CREATE OR REPLACE FUNCTION public.mk_grants_to_schema_roles_by_owner(v_schema_owner text)
  RETURNS void AS
$BODY$
declare
 v_schema_name text;
 v_table_name text;
 v_view_name text;
 v_sequence_name text;
 v_function_name text;
 v_grant_statement text;
begin

-- find all the schemas owned by this userid
for v_schema_name in SELECT n.nspname from pg_namespace n, pg_user u WHERE n.nspowner = u.usesysid and u.usename = v_schema_owner LOOP

   v_grant_statement := 'GRANT usage ON schema ' || v_schema_name || ' to role_write_schema_' || v_schema_name;
   execute v_grant_statement;
   v_grant_statement := 'GRANT usage ON schema ' || v_schema_name || ' to role_read_schema_' || v_schema_name;
   execute v_grant_statement;

      for v_table_name in select schemaname || '.' ||tablename from pg_tables where schemaname = v_schema_name LOOP
         v_grant_statement := 'grant select, update, insert, delete on table ' || v_table_name || ' to role_write_schema_' || v_schema_name;
         execute v_grant_statement;
         v_grant_statement := 'grant select on table ' || v_table_name || ' to role_read_schema_' || v_schema_name;
         execute v_grant_statement;
      END LOOP;

      for v_view_name in select schemaname || '.' || viewname from pg_views where schemaname = v_schema_name LOOP
         v_grant_statement := 'grant select, update, insert, delete on ' || v_view_name || ' to role_write_schema_' || v_schema_name;
         execute v_grant_statement;
         v_grant_statement := 'grant select on ' || v_view_name || ' to role_read_schema_' || v_schema_name;
         execute v_grant_statement;
      END LOOP;

      for v_sequence_name in
        SELECT n.nspname || '.' || c.relname
        FROM pg_class c, pg_namespace n WHERE c.relkind = 'S' AND n.oid = c.relnamespace AND n.nspname = v_schema_name LOOP
         v_grant_statement := 'grant select, usage on sequence ' || v_sequence_name || ' to role_write_schema_' || v_schema_name;
         execute v_grant_statement;
         v_grant_statement := 'grant select on sequence ' || v_sequence_name || ' to role_read_schema_' || v_schema_name;
         execute v_grant_statement;
      END LOOP;

      -- grant execute on regular functions, but NOT trigger functions
      for v_function_name in
        SELECT p.oid::regprocedure from pg_proc p LEFT JOIN pg_namespace n ON p.pronamespace=n.oid where n.nspname = v_schema_name
            and p.oid NOT in (select tgfoid from pg_trigger) LOOP
         v_grant_statement := 'grant execute on function ' || v_function_name || ' to role_write_schema_' || v_schema_name;
         execute v_grant_statement;
      END LOOP;

END LOOP;
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;
