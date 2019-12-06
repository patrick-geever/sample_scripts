#!/bin/bash

# Patrick Geever
# July 2013

echo "START: `date`"

# source Postgres vars
. /opt/PostgreSQL/9.2/pg_env.sh

DBNAME=metricsqa

MAILFILE=/home/logs/prune_old_metrics.client_txn_ack_partitions.mail
LOGFILE=/home/logs/prune_old_metrics.client_txn_ack_partitions.log
#maillist=pgeever@company.com
maillist=dataservices@company.com

target_datestring=`psql --tuples-only ${DBNAME} postgres <<EOF | sed 's/^ //' | sed '$d'
select to_char(date_trunc( 'week', current_date::date - interval '2 months')::date, 'YYYYMMDD');
\q
EOF`

#echo "target_datestring = $target_datestring"

tables_to_drop=`psql --tuples-only ${DBNAME} postgres <<EOF | sed 's/^ //' | sed '$d'
select schemaname || '.' || tablename from pg_tables where tablename like 'client_txn_ack_%_week' and tablename <= 'client_txn_ack_${target_datestring}_week' order by tablename;
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

for i in `echo "${tables_to_drop}"`
do

psql -e --tuples-only ${DBNAME} postgres <<EOF
begin;
drop table ${i};
commit;
\q
EOF

done 

echo "END: `date`"
cat $MAILFILE | mail -s "prune_old_metrics.client_txn_ack_partitions.sh: ${DBNAME} `hostname`" $maillist
cat $MAILFILE >> $LOGFILE

