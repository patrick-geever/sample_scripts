-- Function: metrics.need_indexed_part_table_client_txn_ack(name, name, name, name, timestamp with time zone, text)

-- DROP FUNCTION metrics.need_indexed_part_table_client_txn_ack(name, name, name, name, timestamp with time zone, text);

CREATE OR REPLACE FUNCTION metrics.need_indexed_part_table_client_txn_ack(tg_table_schema name, tg_archive_schema name, tg_table_name name, partitioning_column_name name, needed_partitioning_date timestamp with time zone, partitioning_interval text)
  RETURNS name AS
$BODY$
-- $Header: $
/**
 * This stored procedure checks if the needed partitioning table exists, as if not,
 * it creates it. 
 * It also creates all the indexes, that exist on the parent table renaming it
 * according to the new partition table name. 
 * 
 * Be careful about the maximum length of the object name. 
 * 
 * It is usually to be called from the trigger function like 
 * myschema.ruled_indexed_partition_multiplexer_by_view_day()
 *
 * @param TG_TABLE_SCHEMA - the source (shallow) table schema name
 * @param TG_ARCHIVE_SCHEMA - name of the schema, where the partitioning table should be created
 * @param TG_TABLE_NAME - the source (shallow) table name
 * @param partitioning_column_name - the name of the column, that is used to perform the partitioning (this column should exist in the source table) 
 * @param needed_partitioning_date - the value of the partitioning column, this value is used to determine the name of the needed partitioning table
 * @param partitioning_interval - partitioning interval. can be 'week' or 'month'
 *
 * @author Valentine Gogichashvili
 * Modified by Patrick Geever
 */
DECLARE
  partition_beginning_date CONSTANT date := date_trunc( partitioning_interval, needed_partitioning_date )::date;
  needed_partition_table_name "name";
BEGIN
  -- raise info 'starting partition_multiplexer for %.%, needed table is %, partitioning date is %', TG_TABLE_SCHEMA, TG_TABLE_NAME, needed_partition_table_name, needed_partitioning_date;
  -- calculate the name of the needed table
  -- we start with the beginning of the week (week partitioning)
  needed_partition_table_name := TG_TABLE_NAME || 
    to_char( partition_beginning_date, '_YYYYMMDD_') || partitioning_interval;

  -- check that the needed table exists on the database
  perform 1 
    from pg_class, pg_namespace
   where relnamespace = pg_namespace.oid 
     and relkind = 'r'::"char"
     and relname = needed_partition_table_name
     and nspname = TG_ARCHIVE_SCHEMA;

  if not found then 
    DECLARE
      archive_schema_name_prefix CONSTANT text := quote_ident( TG_ARCHIVE_SCHEMA ) || '.';
      base_schema_name_prefix CONSTANT text := quote_ident( TG_TABLE_SCHEMA ) || '.';
      base_table_name CONSTANT text := base_schema_name_prefix || quote_ident( TG_TABLE_NAME );
      quoted_column_name CONSTANT text := quote_ident( partitioning_column_name );
      partition_beginning_date CONSTANT timestamp := date_trunc( partitioning_interval, needed_partitioning_date )::timestamp;
      next_partition_beginning_date timestamp := date_trunc( partitioning_interval, needed_partitioning_date + ( '1 ' || partitioning_interval )::interval )::timestamp;
      quoted_needed_table_name CONSTANT text := archive_schema_name_prefix || quote_ident ( needed_partition_table_name );
      quoted_rule_name CONSTANT text := quote_ident( 'rule_' || TG_TABLE_NAME || to_char( partition_beginning_date, '_YYYYMMDD') );
      base_table_owner name;
      s text;
      a text;
      parent_index_name text;
      parent_index_has_valid_name boolean;
    BEGIN
--      SET search_path = myschema_partitions, myschema, public;
      SET search_path = metrics, public;
      -- we have to create a needed table now
      -- check if the partitioning date has been passed correctly
      if needed_partitioning_date is null then 
        raise exception 'partitioning_date should not be NULL';
      end if;
      -- check if the partitioning interval is correct
      -- we check it here and not in the trigger function to improve the performance
      if partitioning_interval not in ( 'week', 'month' ) then 
        raise exception $$partitioning_interval is set to [%] and should be 'week' or 'month'$$, partitioning_interval;
      end if;
      -- check for the base table and extract the table owner
      select pg_roles.rolname into base_table_owner
        from pg_class, pg_namespace, pg_roles
       where relnamespace = pg_namespace.oid 
         and relkind = 'r'::"char"
         and relowner = pg_roles.oid
         and relname = TG_TABLE_NAME
         and nspname = TG_TABLE_SCHEMA;
      if not found then 
        raise exception 'cannot find base table %.%', TG_TABLE_SCHEMA, TG_TABLE_NAME;
      end if;
      -- now check that the base table contains the partitioning column
      perform 1 from information_schema.columns where table_schema = TG_TABLE_SCHEMA and table_name = TG_TABLE_NAME and column_name = partitioning_column_name;
      if not found then 
        raise exception 'cannot find partitioning column % in the table %.%', quoted_column_name, TG_TABLE_SCHEMA, TG_TABLE_NAME;
      end if;

      s := $$
        CREATE TABLE $$ || quoted_needed_table_name || $$ (
          CHECK ( $$ || quoted_column_name || $$ >= TIMESTAMPTZ $$ || quote_literal( partition_beginning_date ) || $$ AND 
                  $$ || quoted_column_name || $$ < TIMESTAMPTZ $$ || quote_literal( next_partition_beginning_date ) || $$ )
        ) INHERITS ( $$ || base_table_name || $$ ); $$;
      raise notice 'creating table as [%]', s;
      EXECUTE s;


      -- Add FKs here

      -- Add triggers here


      if coalesce(length(base_table_owner), 0) = 0 then 
        raise exception 'base_table_owner is unknown';
      end if;
      s := $$
        ALTER TABLE $$ || quoted_needed_table_name || 
          $$ OWNER TO $$ || base_table_owner;
      raise notice 'changing owner as [%]', s;
      EXECUTE s;

      s := $$
        GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE $$ || quoted_needed_table_name || 
          $$ TO role_write_schema_metrics ;$$;
      EXECUTE s;

      s := $$
        GRANT SELECT ON TABLE $$ || quoted_needed_table_name || 
          $$ TO role_read_schema_metrics ;$$;
      EXECUTE s;

      -- extract all the indexes existing on the parent table and apply them to the newly created partition
      for a, s, parent_index_name, parent_index_has_valid_name
       in  SELECT CASE indisclustered WHEN TRUE THEN 'ALTER TABLE ' || needed_partition_table_name::text || ' CLUSTER ON ' || replace( i.relname, c.relname, needed_partition_table_name::text ) ELSE NULL END as clusterdef,
                  replace( pg_get_indexdef(i.oid), TG_TABLE_NAME::text, needed_partition_table_name::text ),
                  i.relname,
                  strpos( i.relname, TG_TABLE_NAME::text ) > 0
             FROM pg_index x
             JOIN pg_class c ON c.oid = x.indrelid
             JOIN pg_class i ON i.oid = x.indexrelid
             LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
             LEFT JOIN pg_tablespace t ON t.oid = i.reltablespace
            WHERE c.relkind = 'r'::"char" 
              AND i.relkind = 'i'::"char"
              AND n.nspname = TG_TABLE_SCHEMA
              AND c.relname = TG_TABLE_NAME
      loop
        if parent_index_has_valid_name then 
          if strpos( s, quote_ident( TG_TABLE_SCHEMA ) || '.' ) then 
            raise info 'create index statement contains original schema name, removing it';
            s := replace( s, quote_ident( TG_TABLE_SCHEMA ) || '.', '' );
          end if;
          raise notice 'creating index as [%]', s;
          EXECUTE s;
          if a is not null then 
            if strpos( a, quote_ident( TG_TABLE_SCHEMA ) || '.' ) then 
              raise info 'alter index statement contains original schema name, removing it';
              a := replace( a, quote_ident( TG_TABLE_SCHEMA ) || '.', '' );
            end if;
            raise notice 'setting clustering as [%]', a;
            EXECUTE a;
          end if;
        else 
          raise exception 'parent index name [%] should contain the name of the parent table [%]', parent_index_name, TG_TABLE_NAME;
        end if;
      end loop;

    END;
  end if;
  return needed_partition_table_name;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE STRICT SECURITY DEFINER
  COST 100;
ALTER FUNCTION metrics.need_indexed_part_table_client_txn_ack(name, name, name, name, timestamp with time zone, text)
  OWNER TO metrics;
GRANT EXECUTE ON FUNCTION metrics.need_indexed_part_table_client_txn_ack(name, name, name, name, timestamp with time zone, text) TO metrics;
GRANT EXECUTE ON FUNCTION metrics.need_indexed_part_table_client_txn_ack(name, name, name, name, timestamp with time zone, text) TO public;
GRANT EXECUTE ON FUNCTION metrics.need_indexed_part_table_client_txn_ack(name, name, name, name, timestamp with time zone, text) TO role_write_schema_metrics;

