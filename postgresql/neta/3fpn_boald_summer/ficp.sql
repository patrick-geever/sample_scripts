set nls_date_format = 'YYYY-MM-DD HH24:MI';

\a

\o ficp.lst

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

\o

\a

