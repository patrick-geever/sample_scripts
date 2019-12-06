SET search_path=neta;

create or replace type neta.neta_averages_delete_select_fix_f_type as (
 bm_unit_id character varying(20),
 station_name character varying(36),
 emissions_permit character varying(24),
 rated_power double precision,
 avg_datehour timestamp without time zone,
 output double precision,
 avg3hour numeric,
 avg12hour numeric,
 avg72hour numeric,
 ef_co2 double precision,
 ef_co2_50 double precision,
 ef_co2_ws double precision,
 parent_name character varying(36)
);


CREATE OR REPLACE FUNCTION neta.neta_averages_delete_select_fix_f() RETURNS SETOF neta_averages_delete_select_fix_f_type AS
$BODY$

DECLARE
  r neta.neta_averages_delete_select_fix_f_type;
  x neta.neta_averages_delete_select_fix_f_type;
  v_sql text;
  v_avg3hour numeric;
  v_avg12hour numeric;
  v_avg72hour numeric;

BEGIN

create temporary table neta_co2_temp_table as select * from neta.neta_averages_delete where 1 = 2;

create index neta_co2_temp_table_bm_unit_id_idx on neta_co2_temp_table(bm_unit_id);
create index neta_co2_temp_table_avg_datehour_idx on neta_co2_temp_table(avg_datehour);
analyze neta_co2_temp_table;


insert into neta_co2_temp_table (bm_unit_id, station_name, emissions_permit, rated_power, avg_datehour, output, ef_co2, ef_co2_50, ef_co2_ws, parent_name)
select
A.BM_UNIT_ID, B.station_name, B.emissions_permit, B.nameplate_capacity RATED_POWER, A.AVG_DATEHOUR, A.OUTPUT,
B.ef_co2, B.ef_co2_50, B.ef_co2_ws, B.parent_name
from neta.hourly_output2 A INNER JOIN ACTIVE_UNITS_T_NEW B ON A.bm_unit_id = B.BM_unit_id
where 
A.avg_datehour >= B.online_date
and A.avg_datehour > current_timestamp - interval '5 days'
and not exists (select bm_unit_id,avg_datehour from neta_averages where bm_unit_id = A.bm_unit_id and avg_datehour = A.avg_datehour)
;

for r in select * from neta_co2_temp_table
loop
-- raise notice '%, %, %', r.bm_unit_id, r.AVG_DATEHOUR, r.station_name;

select sum(output) / 3 into v_avg3hour from neta.hourly_output2 where BM_UNIT_ID = r.BM_UNIT_ID and AVG_DATEHOUR <= r.AVG_DATEHOUR and AVG_DATEHOUR > r.AVG_DATEHOUR - interval '3 hours';

select sum(output) / 12 into v_avg12hour from neta.hourly_output2 where BM_UNIT_ID = r.BM_UNIT_ID and AVG_DATEHOUR <= r.AVG_DATEHOUR and AVG_DATEHOUR > r.AVG_DATEHOUR - interval '12 hours';

select sum(output) / 72 into v_avg72hour from neta.hourly_output2 where BM_UNIT_ID = r.BM_UNIT_ID and AVG_DATEHOUR <= r.AVG_DATEHOUR and AVG_DATEHOUR > r.AVG_DATEHOUR - interval '72 hours';
--raise notice 'v_avg72hour = %', v_avg72hour ;

x.bm_unit_id := r.bm_unit_id;
x.station_name := r.station_name;
x.emissions_permit := r.emissions_permit;
x.rated_power := r.rated_power;
x.avg_datehour := r.avg_datehour;
x.output := r.output;
x.avg3hour := v_avg3hour;
x.avg12hour := v_avg12hour;
x.avg72hour := v_avg72hour;
x.ef_co2 := r.ef_co2;
x.ef_co2_50 := r.ef_co2_50;
x.ef_co2_ws := r.ef_co2_ws;
x.parent_name := r.parent_name;


return next x;
end loop;


drop table neta_co2_temp_table;

--RETURN
END;
$BODY$
LANGUAGE 'plpgsql';

