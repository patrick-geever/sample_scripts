set linesize 500
set pagesize 50000
set trimspool on
set heading off
alter session set nls_date_format = 'YYYY-MM-DD HH24:MI';


spool fico.lst


select 
 bm_unit_id  || ',' ||
 fpn_date || ',' ||
 sett_period || ',' ||
 np || ',' ||
 from_time_gmt || ',' ||
 from_level || ',' ||
  to_time_gmt || ',' ||
 to_level 
-- || ',' ||  time_stamp
from neta.fpn_interm_cur
order by 
-- bm_unit_id ,     fpn_date     ,sett_period,np,  from_time_gmt   ,from_level,   to_time_gmt    ,to_level
 fpn_date     ,sett_period,np,  from_time_gmt   ,from_level,   to_time_gmt    ,to_level, bm_unit_id 
;

spool off
