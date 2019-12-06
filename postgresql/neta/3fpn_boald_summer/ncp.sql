set nls_date_format = 'YYYY-MM-DD HH24:MI';

\a

\o ncp.lst


select 
 bm_unit_id || ',' ||
 acceptance_id || ',' ||
  acceptance_time   || ',' ||
 ad || ',' ||
 np || ',' ||
   from_time_gmt    || ',' ||
 from_level || ',' ||
    to_time_gmt     || ',' ||
 to_level || ',' ||
      avg_time
--        || ',' || avg_level 
--        || ',' || time_stamp
from neta.neta_minute_partition_cur
order by bm_unit_id,acceptance_id, acceptance_time  ,ad,np,  from_time_gmt   ,from_level,   to_time_gmt    ,to_level,     avg_time     ,      avg_level
;

\o

\a
