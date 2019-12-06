#!/bin/bash

# Source common variables
. /servers/config/gdr.env.sh


SCRIPT_NAME=`basename $0`; export SCRIPT_NAME
eval $TRACKER_SCRIPT $SCRIPT_NAME $TRACKER_NUMBER $TRACKER_HOSTNAME START


#Created By Stan
#Dated: 10/oct/06 


x=-1
MYDIR=/alpha/neta
mydate=`date --date "$x days" "+%Y-%m-%d"`

#while [ "$mydate" != "2007-02-03" ]
#do

edb-psql -h ${DBHOSTNAME} -p ${DBPORT} -d ${DBNAME} -U ${DBUSER} <<EOF
-- connect neta/gdrprod 	

truncate table neta.neta_minute_partition_cur;
commit;
insert into neta.neta_minute_partition_cur select * from neta.neta_minute_partition_t where trunc(from_time_gmt)>='$mydate';
commit;
truncate table fpn_interm_cur;
commit;
insert into neta.fpn_interm_cur select * from neta.fpn_interm where trunc(from_time_gmt) >='$mydate';
commit;

insert into neta.hourly_output2
select a.* from 
(SELECT AVG_HOUR AVG_DATEHOUR,bm_unit_id, sum(coalesce(boal_avglevel,avg_level)) AVG_LEVEL, sysdate time_stamp  
from  
(
select
  a.bm_unit_id
, a.from_time_gmt
, a.to_time_gmt
--orig oracle , ((a.from_level + a.to_level)/120)*( (a.to_time_gmt - a.from_time_gmt)*24*60) avg_level
, ((a.from_level + a.to_level)/120) * (extract (epoch from a.to_time_gmt - a.from_time_gmt)/60) avg_level
,  sum(case when b.avg_time between a.from_time_gmt and a.to_time_gmt then b.avg_level else null end) boal_avglevel
, TO_DATE(TO_CHAR(a.from_time_gmt,'YYYY-MM-DD HH24'),'YYYY-MM-DD HH24') AVG_HOUR   
from neta.fpn_interm_cur a left outer join neta.neta_minute_partition_cur b on a.bm_unit_id = b.bm_unit_id	   
where (a.from_level + a.to_level) >=0      
group by
  a.bm_unit_id
, a.from_time_gmt
, a.to_time_gmt
--orig oracle , ((a.from_level + a.to_level)/120)*( (a.to_time_gmt - a.from_time_gmt)*24*60)
, ((a.from_level + a.to_level)/120) * (extract(epoch from a.to_time_gmt - a.from_time_gmt)/60)
, SETT_PERIOD
, FPN_DATE
)   
GROUP BY AVG_HOUR,bm_unit_id  
having sum(coalesce(boal_avglevel,avg_level))<>0)a
left outer join (select * from neta.hourly_output2)b
on a.bm_unit_id = b.bm_unit_id and a.avg_datehour = b.avg_datehour
where b.bm_unit_id is null;
commit;
EOF

x=$(($x-1))
mydate=`date --date "$x days" "+%Y-%m-%d"`
#done

# Run tracker script for end
eval $TRACKER_SCRIPT $SCRIPT_NAME $TRACKER_NUMBER $TRACKER_HOSTNAME END

