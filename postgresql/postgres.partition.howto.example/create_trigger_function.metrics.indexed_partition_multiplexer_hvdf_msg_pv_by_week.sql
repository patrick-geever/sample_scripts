-- Function: metrics.indexed_partition_multiplexer_hvdf_msg_pv_by_week()

-- DROP FUNCTION metrics.indexed_partition_multiplexer_hvdf_msg_pv_by_week();

CREATE OR REPLACE FUNCTION metrics.indexed_partition_multiplexer_hvdf_msg_pv_by_week()
  RETURNS trigger AS
$BODY$
-- $Header: $
/**
 * This is a common trigger function that can be used to partition any table
 * that has a insert_timestamp partitioning column.
 * This function will only work on BEFORE INSERT row level triggers.
 * If the first parameter is specified, it can only be 'week' or 'month'
 * to indicate the needed partitioning schedule.
 */
DECLARE
  schema_name_prefix CONSTANT text := quote_ident( TG_TABLE_SCHEMA ) || '.';
  table_name_prefix CONSTANT text := TG_TABLE_NAME || '_';
  needed_month_table_name text;
  partitioning_interval CONSTANT text := coalesce( TG_ARGV[0], 'week' );
  s text;
BEGIN
  if not ( TG_WHEN = 'BEFORE' and TG_LEVEL = 'ROW' and TG_OP = 'INSERT' ) then
    raise exception 'This trigger function can only be used with BEFORE INSERT row level triggers!';
  end if;
  -- raise info 'starting partition_multiplexer for %.%', TG_TABLE_SCHEMA, TG_TABLE_NAME;
  if new.insert_timestamp is null then
    raise exception 'partitioning column "insert_timestamp" cannot be NULL';
  end if;

  needed_month_table_name :=
    metrics.need_indexed_partition_table_hvdf_msg_pv(
      TG_TABLE_SCHEMA,
      "name" 'metrics',
      TG_TABLE_NAME,
      "name" 'insert_timestamp',
      new.insert_timestamp, partitioning_interval );

  -- raise info 'needed_month_table_name is %', needed_month_table_name;
  select new into s;

 s := $$INSERT INTO metrics.$$ || needed_month_table_name ||
       $$ SELECT ($$ || quote_literal( s ) || $$::$$ ||
          schema_name_prefix || TG_TABLE_NAME || $$).*  $$;

 -- raise info 'executing statement [%]', s;
  EXECUTE s;
  RETURN NULL;
END;
-- ChangeLog:
-- $Log: $
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION metrics.indexed_partition_multiplexer_hvdf_msg_pv_by_week()
  OWNER TO metrics;
