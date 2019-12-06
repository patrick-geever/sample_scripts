SET search_path=coal;


create or replace type coal.v_coal_ratios_type as (
 doe_id numeric(12,0),
 entity_id numeric(10,0),
 plant_name varchar(64),
 period varchar(43),
 mwh_coal double precision,
 mwh_other double precision,
 coal_burns double precision,
 coal_ratio double precision,
 coal_ratio_6mth_avg double precision,
 coal_factor double precision,
 coal_factor_6mth_avg double precision
);

create or replace type coal.v_coal_ratios_query_type as (
 doe_id numeric(12,0),
 entity_id numeric(10,0),
 plant_name varchar(64),
 period varchar(43),
 mwh_coal double precision,
 mwh_other double precision,
 coal_burns double precision,
 coal_ratio double precision,
 coal_factor double precision
);

CREATE OR REPLACE FUNCTION coal.v_coal_ratios_f() RETURNS SETOF v_coal_ratios_type AS
$BODY$
DECLARE
  r coal.v_coal_ratios_query_type%rowtype;
  x coal.v_coal_ratios_type%rowtype;
  v_entity_id numeric(10,0);
  v_period varchar(43);
  v_coal_ratio_6mth_avg double precision;
  v_coal_factor_6mth_avg double precision;
BEGIN

for r in 
select
  doe_id,
  entity_id,
  plant_name,
  period,
  mwh_coal,
  mwh_other,
  coal_burns,
  case when mwh_coal = 0  then NULL else (case when coal_burns/mwh_coal  between .3 and .9 then coal_burns/mwh_coal else NULL end) end as coal_ratio,
  case when (mwh_coal+nvl(mwh_other::numeric,0)) = 0 then NULL else mwh_coal/(mwh_coal+nvl(mwh_other::numeric,0)) end as coal_factor 
  from coal.v_eiacoal_vs_others order by period desc
loop

  -- select last 5 rows back in time for 6 month averages. fix for Oracle syntax:
  -- over (partition by entity_id order by period rows between 5 preceding and 0 preceding)
  select 
  avg(case when mwh_coal = 0 then NULL else (case when coal_burns/mwh_coal between .3 and .9 then coal_burns/mwh_coal else NULL end) end),
  avg(case when (mwh_coal+nvl(mwh_other::numeric,0)) = 0 then NULL else   mwh_coal/(mwh_coal+nvl(mwh_other::numeric,0)) end)
  into v_coal_ratio_6mth_avg, v_coal_factor_6mth_avg 
  from coal.v_eiacoal_vs_others where entity_id = r.entity_id and to_timestamp(period,'YYYY-MM') <= to_timestamp(r.period,'YYYY-MM') 
  and to_timestamp(period,'YYYY-MM') >= to_timestamp(r.period,'YYYY-MM') - interval '5 months';

  x.doe_id := r.doe_id;
  x.entity_id := r.entity_id;
  x.plant_name := r.plant_name;
  x.period := r.period;
  x.mwh_coal := r.mwh_coal;
  x.mwh_other := r.mwh_other;
  x.coal_burns := r.coal_burns;
  x.coal_ratio := r.coal_ratio;
  x.coal_ratio_6mth_avg := v_coal_ratio_6mth_avg; 
  x.coal_factor := r.coal_factor;
  x.coal_factor_6mth_avg := v_coal_factor_6mth_avg;

  return next x;
end loop;
RETURN;
END;
$BODY$
LANGUAGE 'plpgsql';


