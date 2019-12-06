-- Function: metrics.tr_f_client_txn_ack_partition()

-- DROP FUNCTION metrics.tr_f_client_txn_ack_partition();

CREATE OR REPLACE FUNCTION metrics.tr_f_client_txn_ack_partition()
  RETURNS trigger AS
$BODY$
-- $Header: $
/**
 * This is a common trigger function that can be used to partition any table 
 * that has a gasday partitioning column.
 * This function will only work on BEFORE INSERT row level triggers.
 * If the first parameter is specified, it can only be 'week' or 'month' 
 * to indicate the needed partitioning schedule.
 *
 * @author Valentine Gogichashvili
 * Modified by Patrick Geever
 */
DECLARE
  schema_name_prefix CONSTANT text := quote_ident( TG_TABLE_SCHEMA ) || '.';
  table_name_prefix CONSTANT text := TG_TABLE_NAME || '_';
  needed_month_table_name text;
  partitioning_interval CONSTANT text := coalesce( TG_ARGV[0], 'week' );
  s text;
  date_string text;
BEGIN
  if not ( TG_WHEN = 'BEFORE' and TG_LEVEL = 'ROW' and TG_OP = 'INSERT' ) then 
    raise exception 'This trigger function can only be used with BEFORE INSERT row level triggers!';
  end if;
  -- raise info 'starting partition_multiplexer for %.%', TG_TABLE_SCHEMA, TG_TABLE_NAME;
  if new.last_modified is null then 
    raise exception 'partitioning column "last_modified" cannot be NULL';
  end if;

  -- pg
  -- Do insert into right partition first. If it succeeds we are done without having to call metrics.need_indexed_part_table_client_txn_ack everytime.
  -- If no table, trap into exception, run metrics.need_indexed_part_table_client_txn_ack to create table, then insert

   -- look at new.last_modified to determine which partition to insert into.
   date_string :=  to_char( date_trunc( partitioning_interval::text, new.last_modified::TIMESTAMP ),  '_YYYYMMDD_') || partitioning_interval;

 begin

   needed_month_table_name := TG_TABLE_NAME || date_string;

  --raise info 'needed_month_table_name is %', needed_month_table_name;
  select new into s;

  s := $$INSERT INTO metrics.$$ || needed_month_table_name || 
        $$ SELECT ($$ || quote_literal( s ) || $$::$$ || 
           schema_name_prefix || TG_TABLE_NAME || $$).*  $$;
 
  --raise info 'executing statement [%]', s;

  EXECUTE s;

  EXCEPTION
      -- if table does not exist then call need_indexed_part_table_fact_01() function to create partition table, then insert to new table.
      when undefined_table then
  --raise notice 'EXCEPTION: undefined_table';

   needed_month_table_name :=
     metrics.need_indexed_part_table_client_txn_ack(
       TG_TABLE_SCHEMA,
       "name" 'metrics',
       TG_TABLE_NAME,
       "name" 'last_modified',
       new.last_modified, partitioning_interval );

  s := '';
  select new into s;
  s := $$INSERT INTO metrics.$$ || needed_month_table_name ||
        $$ SELECT ($$ || quote_literal( s ) || $$::$$ ||
           schema_name_prefix || TG_TABLE_NAME || $$).*  $$;

  
   --raise info 'executing statement [%]', s;
   EXECUTE s;
 end;

  RETURN NULL;
END;
-- ChangeLog:
-- $Log: $
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION metrics.tr_f_client_txn_ack_partition()
  OWNER TO metrics;

