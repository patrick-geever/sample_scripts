SET search_path=neta;

create or replace type neta.neta_boald_interm_cur_select_fix_f_type as (
 bm_unit_id character varying(30), 
 avg_time timestamp without time zone,
 acceptance_id character varying(30),
 acceptance_time timestamp without time zone,
 ad character varying(5),
 np numeric,
 from_time_gmt timestamp without time zone,
 from_level numeric, 
 to_time_gmt timestamp without time zone,
 to_level numeric,
 time_stamp timestamp without time zone
);


CREATE OR REPLACE FUNCTION neta.neta_boald_interm_cur_select_fix_f(v_original_date timestamp) 
RETURNS SETOF neta_boald_interm_cur_select_fix_f_type AS
$BODY$

DECLARE
  x neta.neta_boald_interm_cur_select_fix_f_type%rowtype;
  j text;
  v_bm_unit_id text;
  v_acceptance_id character varying(30);
  v_acceptance_time timestamp without time zone;
  v_ad character varying(5);
  v_np numeric;
  v_from_time_gmt timestamp without time zone;
  v_from_level numeric;
  v_to_time_gmt timestamp without time zone;
  v_to_level numeric;


BEGIN


create temporary table neta_boald_interm_cur_select_fix_f_temp_table as select * from neta.BOALD_INTERM_CUR where 1 = 2;

insert into neta_boald_interm_cur_select_fix_f_temp_table
select  bm_unit_id, acceptance_id, acceptance_time, ad, np, from_time_gmt, from_level, to_time_gmt, to_level
from neta.BOALD_INTERM_CUR
where v_original_date between from_time_gmt and to_time_gmt
and from_time_gmt between (v_original_date - 2/24) and v_original_date
and to_time_gmt between v_original_date and (v_original_date + 2/24)
and (from_level + to_level) >= 0
order by bm_unit_id
;


for j in select distinct bm_unit_id from neta_boald_interm_cur_select_fix_f_temp_table order by bm_unit_id
loop

select max(acceptance_time) into v_acceptance_time from neta_boald_interm_cur_select_fix_f_temp_table where bm_unit_id = j;
  
select max(acceptance_id) into v_acceptance_id from neta_boald_interm_cur_select_fix_f_temp_table where bm_unit_id = j and acceptance_time = v_acceptance_time;

select max(ad) into v_ad from neta_boald_interm_cur_select_fix_f_temp_table where bm_unit_id = j and acceptance_time = v_acceptance_time;

select max(np) into v_np from neta_boald_interm_cur_select_fix_f_temp_table where bm_unit_id = j and acceptance_time = v_acceptance_time; 

select max(from_time_gmt) into v_from_time_gmt from neta_boald_interm_cur_select_fix_f_temp_table where bm_unit_id = j and acceptance_time = v_acceptance_time; 

select max(from_level) into v_from_level from neta_boald_interm_cur_select_fix_f_temp_table where bm_unit_id = j and acceptance_time = v_acceptance_time;

select max(to_time_gmt) into v_to_time_gmt from neta_boald_interm_cur_select_fix_f_temp_table where bm_unit_id = j and acceptance_time = v_acceptance_time; 

select max(to_level) into v_to_level from neta_boald_interm_cur_select_fix_f_temp_table where bm_unit_id = j and acceptance_time = v_acceptance_time; 

  x.bm_unit_id := j;
  x.avg_time := v_original_date;
  x.acceptance_id := v_acceptance_id;
  x.acceptance_time := v_acceptance_time;
  x.ad := v_ad;
  x.np := v_np;
  x.from_time_gmt := v_from_time_gmt;
  x.from_level := v_from_level;
  x.to_time_gmt := v_to_time_gmt;
  x.to_level := v_to_level;
  x.time_stamp := sysdate;

  return next x;

end loop;

drop table neta_boald_interm_cur_select_fix_f_temp_table;

RETURN;
END;
$BODY$
LANGUAGE 'plpgsql';

