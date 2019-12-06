#!/bin/bash

# Patrick Geever
# April 2013

MAILFILE=/home/logs/prune_old_hvdf_partitions.metricsqa.mail
LOGFILE=/home/logs/prune_old_hvdf_partitions.metricsqa.log
maillist=dataservices@company.com
#maillist=pgeever@company.com

echo "START: `date`"

# source Postgres vars
. /opt/PostgreSQL/9.2/pg_env.sh

DBNAME=metricsqa

target_datestring=`psql --tuples-only metricsqa postgres <<EOF | sed 's/^ //' | sed '$d'
select to_char(date_trunc( 'week', current_date::date - interval '8 weeks')::date, 'YYYYMMDD');
\q
EOF`

#echo "target_datestring = $target_datestring"

tables_to_drop=`psql --tuples-only ${DBNAME} postgres <<EOF | sed 's/^ //' | sed '$d'
select schemaname || '.' || tablename from pg_tables where tablename like 'hvdf_msg_pv_%_week' and tablename <= 'hvdf_msg_pv_${target_datestring}_week' order by tablename;
\q
EOF`

# check for an empty tables_to_drop string, if zero length exit
if [ -z "${tables_to_drop}" ]
then
        echo "No tables to drop: Exiting"
        echo "END: `date`"
        cat $MAILFILE >> $LOGFILE
        exit
fi

echo "tables to drop"
echo "${tables_to_drop}"


# These 2 partition tables, for messages and batchs, must be dropped as a pair. The messages table is a child table of batchs and must be dropped first. 
for i in `echo "${tables_to_drop}"`
do

batch_table_name=`echo ${i} | sed 's/hvdf_msg_pv/hvdf_batch_pv/'`

psql -e --tuples-only ${DBNAME} postgres <<EOF
begin;
drop table ${i};
drop table ${batch_table_name};
--commit;
rollback;
\q
EOF

done 


echo "END: `date`"
cat $MAILFILE | mail -s "prune_old_hvdf_partitions.metricsqa.sh: `hostname`" $maillist
cat $MAILFILE >> $LOGFILE

