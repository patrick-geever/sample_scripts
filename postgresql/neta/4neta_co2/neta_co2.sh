#!/bin/bash

# Source common variables
. /servers/config/gdr.env.sh

SCRIPT_NAME=`basename $0`; export SCRIPT_NAME
eval $TRACKER_SCRIPT $SCRIPT_NAME $TRACKER_NUMBER $TRACKER_HOSTNAME START

#Created By Stan
#Dated: 5/feb/07 

echo "Start: `date`"

edb-psql -h ${DBHOSTNAME} -p ${DBPORT} -d ${DBNAME} -U ${DBUSER} <<EOF
-- connect neta/gdrqa

--pg Old code to insert 0 output back to beginning of time
--insert into neta.hourly_output2
--select b.avg_datehour,a.bm_unit_id,0,sysdate 
--from 
--(select distinct bm_unit_id from neta.hourly_output2 ) a,
--(select distinct avg_datehour from neta.hourly_output2) b 
--where  not exists (select * from neta.hourly_output2 where bm_unit_id = a.bm_unit_id and avg_datehour = b.avg_datehour);
----commit;


-- pg New code to add 0 output only for c02 plants back 3 days
insert into neta.hourly_output2
select b.avg_datehour,a.bm_unit_id,0,sysdate 
from 
( select distinct bm_unit_id from active_units_t_new where include = 1 and bm_unit_id <> 'T_BPGRD-1') a,
( select ((select max(avg_datehour) from neta.hourly_output2) - interval '3 days') + x * '1 hour'::interval as avg_datehour from generate_series(0,72) x) b 
where not exists (select avg_datehour from neta.hourly_output2 where bm_unit_id = a.bm_unit_id and avg_datehour = b.avg_datehour and avg_datehour >= current_date  - interval '3 days')
;


truncate table neta_averages_delete;

insert into neta_averages_delete 
select *, sysdate from neta.neta_averages_delete_select_fix_f();

-- function neta.neta_averages_delete_select_fix_f() replaces original Oracle code below
--
--insert into neta_averages_delete 
--select 
--A.BM_UNIT_ID, B.station_name, B.emissions_permit, B.nameplate_capacity RATED_POWER, A.AVG_DATEHOUR, A.OUTPUT, 
--AVG(A.OUTPUT) OVER (PARTITION BY A.BM_UNIT_ID ORDER BY A.AVG_DATEHOUR ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AVG3HOUR,
--AVG(A.OUTPUT) OVER (PARTITION BY A.BM_UNIT_ID ORDER BY A.AVG_DATEHOUR ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AVG12HOUR,
--AVG(A.OUTPUT) OVER (PARTITION BY A.BM_UNIT_ID ORDER BY A.AVG_DATEHOUR ROWS BETWEEN 71 PRECEDING AND CURRENT ROW)  AVG72HOUR,
--B.ef_co2, B.ef_co2_50, B.ef_co2_ws, B.parent_name
--from neta.hourly_output2 A INNER JOIN ACTIVE_UNITS_T_NEW B ON A.bm_unit_id = B.BM_unit_id 
--where not exists (select * from neta.neta_averages where bm_unit_id = A.bm_unit_id and avg_datehour = A.avg_datehour) 
--and  A.avg_datehour >= B.online_date;
commit;

insert into neta.neta_averages
select bm_unit_id,station_name,emissions_permit,rated_power,avg_datehour,avg(output)output
,avg(avg3hour)avg3hour,avg(avg12hour),avg(avg72hour)
,avg(ef_co2)ef_co2,avg(ef_co2_50)ef_co2_50,avg(ef_co2_ws)ef_co2_ws,parent_name, sysdate
 from neta.neta_averages_delete
 group by bm_unit_id,station_name,emissions_permit,rated_power,avg_datehour,parent_name;
 commit;


 BEGIN
 DBMS_OUTPUT.PUT_LINE('START TIME: '||SYSDATE);
 FOR ITEM IN (select distinct bm_unit_id from neta.active_units_t_new where include = 1 and bm_unit_id <> 'T_BPGRD-1')
 LOOP
 neta.UPDATE_NETA_OP_LEVEL(ITEM.BM_UNIT_ID,SYSDATE-1,SYSDATE-1);
 END LOOP;
 DBMS_OUTPUT.PUT_LINE('END TIME: '||SYSDATE);
 END;
/

------------------------------------------------------------------

insert into neta.neta_co2_t 
select a.*,sysdate from neta.neta_co2 a 
where exists ( 
select * from neta.neta_co2_t where a.avg_datehour > (select max(avg_datehour) from neta.neta_co2_t)
); 
commit;


delete from euro_fuels.neta_co2_daily_t where avg_date = to_char(sysdate-1,'YYYY-MM-DD');

commit;

insert into euro_fuels.neta_co2_daily_t 
select a.* from euro_fuels.neta_co2_daily a 
where exists 
(select * from euro_fuels.neta_co2_daily_t where a.avg_date > (select max(avg_date) from euro_fuels.neta_co2_daily_t)
); 
commit; 


EOF

echo "End: `date`"

# Run tracker script for end
eval $TRACKER_SCRIPT $SCRIPT_NAME $TRACKER_NUMBER $TRACKER_HOSTNAME END
