#!/bin/bash

# Source common variables
. /servers/config/gdr.env.sh

# Get script name and run tracker script
SCRIPT_NAME=`basename $0`; export SCRIPT_NAME
eval $TRACKER_SCRIPT $SCRIPT_NAME $TRACKER_NUMBER $TRACKER_HOSTNAME START

#Created By Stan
#Dated: 7/feb/07 

x=1
MYDIR=/alpha/neta
#for x in `seq 1 4`
#{
rm -f $MYDIR/boald2.csv
#mydate=2009-11-05

mydate=`date --date "-$x days" "+%Y-%m-%d"`
echo "http://www.bmreports.com/tibcodata/tib_messages.$mydate.gz"

wget -U "Windows IE 6.0" -q -O - "http://www.bmreports.com/tibcodata/tib_messages.$mydate.gz" | gunzip -c -q | grep 'BOALF' | sed 's/^[^.]*\.BM\.//g;s/\.BOALF, message={/,/g;s/[A-Z][A-Z]=//g;s/}$//g;s/:GMT//g' | awk -F, '{ for( x = 1; x < 2*($6); x=x+2) print $1 "," $2 "," $3 "," $4  "," $5 "," $6 "," $(x+6) "," $(x+7) }' | perl -e 'while (<>) { s/(\d)(\d)(\d)(\d):(\d)(\d):(\d)(\d):/$1$2$3$4-$5$6-$7$8 /g; print "$_"; }' > $MYDIR/boald.csv

#while read line
#do
#  echo $line,$mydate >> $MYDIR//boald2.csv
#  
#done < $MYDIR/boald.csv
#sqlldr neta/gdrprod control=$MYDIR/tib_boald_rev.ctl data=$MYDIR/boald.csv errors=9999 log=$MYDIR/tib_boald.log bad=$MYDIR/tib_boald.bad

## pg for debugging
edb-psql -h ${DBHOSTNAME} -p ${DBPORT} -d ${DBNAME} -U ${DBUSER} <<EOF
-- connect neta/gdrprd;
--pg need to track this value for debugging before load
\echo select max(time_stamp) from neta.tib_boald_rev
select max(time_stamp) from neta.tib_boald_rev;
select now();
EOF

# data file = boald.csv, loading table = neta.tib_boald_rev_preload
edbldr -d ${DBNAME} -p ${DBPORT} userid=${DBUSER}/${DBPASSWORD} control=$MYDIR/tib_boald_rev.ctl errors=9999 log=$MYDIR/tib_boald.log 

edb-psql -h ${DBHOSTNAME} -p ${DBPORT} -d ${DBNAME} -U ${DBUSER} <<EOF
--connect neta/gdrprod

-- load neta.tib_boald_rev WITHOUT duplicate time_gmt rows
insert into neta.tib_boald_rev
select bm_unit_id, acceptance_id,so,acceptance_time,ad,np,time_gmt, va_level, time_stamp from
(
select bm_unit_id, acceptance_id,so,acceptance_time,ad,np,time_gmt, va_level, time_stamp,
row_number() over (partition by bm_unit_id, acceptance_id,so,acceptance_time,ad,np,time_gmt order by time_gmt, va_level desc) as row_number
from neta.tib_boald_rev_preload
) where row_number = 1;

truncate neta.tib_boald_rev_preload;

--pg need to track this value for debugging before insert
\echo select max(time_stamp) from neta.boald_interm
select max(time_stamp) from neta.boald_interm;
select now();

insert into neta.boald_interm 
        select * from 
	(select bm_unit_id,acceptance_id,acceptance_time,ad,np,time_gmt from_time_gmt,va_level from_level, 
	lead(time_gmt) over (partition by bm_unit_id,acceptance_id,acceptance_time,ad,np order by time_gmt) to_time_gmt, 
	lead(va_level) over (partition by bm_unit_id,acceptance_id,acceptance_time,ad,np order by time_gmt) to_level, 
	sysdate time_stamp 
	from neta.tib_boald_rev
	where trunc(time_gmt) = trunc(to_date('$mydate','YYYY-MM-DD'))
	and time_stamp > (select max(time_stamp) from neta.boald_interm)) 
	where to_time_gmt is not null; 
	commit;

insert into neta.boald_interm 
        select * from 
	(select bm_unit_id,acceptance_id,acceptance_time,ad,np,time_gmt from_time_gmt,va_level from_level, 
	lead(time_gmt) over (partition by bm_unit_id,acceptance_id,acceptance_time,ad,np order by time_gmt) to_time_gmt, 
	lead(va_level) over (partition by bm_unit_id,acceptance_id,acceptance_time,ad,np order by time_gmt) to_level, 
	sysdate time_stamp 
	from neta.tib_boald_rev
	where trunc(time_gmt) = trunc(sysdate))
	where to_time_gmt is not null; 
	commit;

DECLARE
original_date DATE DEFAULT TO_DATE('$mydate 23:59','YYYY-MM-DD HH24:MI');
	
	mcur CURSOR (mcur_original_date timestamp) is
	select bm_unit_id,acceptance_id,acceptance_time,ad,np,from_time_gmt,from_level,to_time_gmt,to_level, avg_time,(to_level + from_level)/120 avg_level,
	time_stamp 
	from
	(

                select * from neta.neta_boald_interm_cur_select_fix_f(mcur_original_date)
                /* 
		 function neta.neta_boald_interm_cur_select_fix_f() replaces the following original Oracle code
		 *
		select bm_unit_id, original_date avg_time, 
		max(acceptance_id) keep (dense_rank last order by acceptance_time) acceptance_id,
		max(acceptance_time) keep (dense_rank last order by acceptance_time) acceptance_time,
		max(ad) keep (dense_rank last order by acceptance_time) ad,
		max(np) keep (dense_rank last order by acceptance_time) np,
		max(from_time_gmt) keep (dense_rank last order by acceptance_time) from_time_gmt,
		max(from_level) keep (dense_rank last order by acceptance_time) from_level,
		max(to_time_gmt) keep (dense_rank last order by acceptance_time) to_time_gmt,
		max(to_level) keep (dense_rank last order by acceptance_time) to_level,
		sysdate time_stamp
		from neta.BOALD_INTERM_CUR
		where original_date between from_time_gmt and to_time_gmt
		and from_time_gmt between (original_date - 2/24) and original_date
		and to_time_gmt between original_date and (original_date + 2/24)
		and (from_level + to_level) >= 0
		group by bm_unit_id ,original_date
                */
);
	TYPE minute_partition_t is TABLE OF neta.neta_minute_partition_t%ROWTYPE INDEX BY BINARY_INTEGER;
	minute_partition minute_partition_t;
	
	
BEGIN
	-- delete from neta.BOALD_INTERM_CUR; 
	truncate neta.BOALD_INTERM_CUR; 
	COMMIT; 
	INSERT INTO neta.BOALD_INTERM_CUR SELECT * FROM neta.BOALD_INTERM WHERE trunc(from_time_gmt) between trunc(original_date)-1 and trunc(original_date)+1;
	COMMIT;  
	WHILE (original_date >= to_date('$mydate 00:00','YYYY-MM-DD HH24:MI'))
	LOOP
	IF trunc(original_date) <> trunc(original_date + 1/1440) 
	THEN 
		delete from neta.BOALD_INTERM_CUR; --NEW DATE APPEARS
		COMMIT; 
		INSERT INTO neta.BOALD_INTERM_CUR SELECT * FROM neta.BOALD_INTERM WHERE trunc(from_time_gmt) between trunc(original_date)-1 and trunc(original_date)+1;
		COMMIT;  
	END IF;
	--original Oracle code: OPEN mcur; 
	--pg dbms_output.put_line('original_date = ' || original_date);
	OPEN mcur(original_date);
	FETCH mcur BULK COLLECT INTO minute_partition;
	CLOSE mcur;
	FOR i IN 1 .. minute_partition.COUNT	
	LOOP
		
		insert into neta.neta_minute_partition_t values(minute_partition(i).bm_unit_id,minute_partition(i).acceptance_id,minute_partition(i).acceptance_time,minute_partition(i).ad,minute_partition(i).np,minute_partition(i).from_time_gmt,minute_partition(i).from_level,minute_partition(i).to_time_gmt,minute_partition(i).to_level,minute_partition(i).avg_time,minute_partition(i).avg_level,minute_partition(i).time_stamp);
		--commit;
		
	END LOOP;
commit;
	original_date:= original_date - 1/1440;	
	
	END LOOP;	
END;
EOF


#}

# Run tracker script for end
eval $TRACKER_SCRIPT $SCRIPT_NAME $TRACKER_NUMBER $TRACKER_HOSTNAME END

