set search_path = aws_weather;


drop TRIGGER aws_hourly_insert_trigger;

---------------


CREATE OR REPLACE FUNCTION aws_weather.aws_hourly_insert_trigger_f()
RETURNS TRIGGER AS $$

DECLARE
  v_sql TEXT;
  v_sql1 TEXT;
  v_sql2 TEXT;
  v_month TEXT;
  v_year TEXT;
  v_quarter TEXT;
BEGIN

v_month := extract(month from NEW.awsdate);
v_year :=  extract(year from NEW.awsdate);

 RAISE NOTICE 'v_month = %', v_month;

if v_month = 1 or v_month = 2 or v_month = 3
then
   v_quarter := 'Q1';
elsif v_month = 4 or v_month = 5 or v_month = 6
then
   v_quarter := 'Q2';
elsif v_month = 7 or v_month = 8 or v_month = 9
then
   v_quarter := 'Q3';
elsif v_month = 10 or v_month = 11 or v_month = 12
then
   v_quarter := 'Q4';
end if;
	
      v_sql1 := 'INSERT INTO aws_weather.AWS_HOURLY_' || v_year || '_' || v_quarter || '(';
      v_sql2 := 'station_cd, estdate, esthour, awsdate, temperature, humidity, pressure, wind_speed, wind_direction, 
               daily_rain, monthly_rain, temperature_rate, humidity_rate, pressure_rate, rain_rate, max_temperature,
               min_temperature, time_stamp, source
        ) VALUES (''' ||
        NEW.station_cd || ''',''' ||
        NEW.estdate || ''',' ||
        NEW.esthour || ',''' ||
        NEW.awsdate || ''',' ||
        coalesce(NEW.temperature::text, 'null') || ',' ||
        coalesce(NEW.humidity::text, 'null') || ',' ||
        coalesce(NEW.pressure::text, 'null') || ',' ||
        coalesce(NEW.wind_speed::text, 'null') || ',' ||
        coalesce(NEW.wind_direction::text, 'null') || ',' ||
        coalesce(NEW.daily_rain::text, 'null') || ',' ||
        coalesce(NEW.monthly_rain::text, 'null') || ',' ||
        coalesce(NEW.temperature_rate::text, 'null') || ',' ||
        coalesce(NEW.humidity_rate::text, 'null') || ',' ||
        coalesce(NEW.pressure_rate::text, 'null') || ',' ||
        coalesce(NEW.rain_rate::text, 'null') || ',' ||
        coalesce(NEW.max_temperature::text, 'null') || ',' ||
        coalesce(NEW.min_temperature::text, 'null') || ',' ||
        coalesce(quote_literal(NEW.time_stamp::text), 'null') || ',' ||
        coalesce(quote_literal(NEW.source::text), 'null') || ');';

       v_sql := v_sql1 || v_sql2;
       RAISE NOTICE 'good - v_sql = %', v_sql;

      EXECUTE v_sql;
      -- RETURN NULL;
      -- RETURN NEW;
      RETURN NULL;


-- Exception handler to manage any rows that would be out of bounds for this table
-- to insert into a catch all exceptions partition
--   EXCEPTION
--     WHEN unique_violation THEN
--       RAISE NOTICE '%', sqlerrm;
--       RETURN NULL;
--     WHEN OTHERS THEN
--       RAISE NOTICE '%', sqlerrm;
--       RAISE NOTICE 'Inserting in aws_weather.AWS_HOURLY_OVERFLOW';
--       v_sql := 'INSERT INTO aws_weather.AWS_HOURLY_OVERFLOW(' || v_sql2;
--       EXECUTE v_sql;
--       RETURN NULL;
END;
$$
LANGUAGE plpgsql;


-- ################################################


CREATE TRIGGER aws_hourly_insert_trigger
    before INSERT ON aws_weather.aws_hourly
    FOR EACH ROW EXECUTE PROCEDURE aws_hourly_insert_trigger_f();

