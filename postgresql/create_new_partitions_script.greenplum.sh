#!/bin/bash

# Patrick Geever
# Dec. 2012

# pass in: 
# Schema.Tablename
# or
# Schema.Tablename End_Date 
# If no end date is passed in then the default end date will = "now() plus 2 months"
# or
# Special Parameter: ALL will do all tables out 2 months
# Date format MUST be "YYYY-MM-DD hh:mi:ss"

if [ ${#} -eq 2 ]
then
	# get schema.table date
	schemaname_parm=`echo $1 | cut -d '.' -f 1`
	tablename_parm=`echo $1 | cut -d '.' -f 2`
	# get date
	date_parm=$2

elif [ ${#} -eq 1 ]
then
	# is this ALL?
	if [ ${1} = "ALL" ]
	then
		all_flag=1
		extend_interval="2 months"
	else
		schemaname_parm=`echo $1 | cut -d '.' -f 1`
		tablename_parm=`echo $1 | cut -d '.' -f 2`
		extend_interval="2 months"
	fi
else 
	echo "
Usage: 
$0 Schema.Tablename
or
$0 Schema.Tablename End_Date
or
$0 ALL

Schemaname.Tablename must be be provided for a single table
If no date is provided the named table will be extended until 2 months from the current_date
End_Date must be in the format: \"YYYY-MM-DD hh:mi:ss\", note the quotes
The "ALL" parameter will extend all partitioned tables until 2 months from current_date
"
exit
fi


# Set variables
#DBNAME=pjgeev
#DBUSER=pjgeev

DBNAME=insightdev
DBUSER=gpadmin

#script_out=~/mk_new_partitions_script.`date "+%Y-%m-%d"`.sql
script_out=mk_new_partitions_script.`date "+%Y-%m-%d"`.sql

# setup up the end date
if [ "${extend_interval}" = "2 months" ]
then
	needed_end_date=`date "+%Y-%m-%d %T" --date '2 months'`	
else
	needed_end_date="$date_parm"
fi
echo "-- needed_end_date = ${needed_end_date}" | tee ${script_out}
#echo "-- needed_end_date = ${needed_end_date}" > ${script_out}

echo "begin;" | tee -a ${script_out}

#SELECT tablename, partitionrank, partitionrangestart, partitionrangeend,
#partitionboundary from pg_partitions where (tablename, partitionrank) in
#(SELECT tablename, max(partitionrank) from pg_partitions group by tablename);

# SELECT tablename, partitionrank, partitionrangestart, partitionrangeend from pg_partitions where (tablename, partitionrank) in (SELECT tablename, max(partitionrank) from pg_partitions group by tablename) and tablename = 'fact_01_qa'; 


if [ $[all_flag] -eq 1  ]
then
#find all partitioned tables to create list to run through
all_partitions=`psql --tuples-only ${DBNAME} ${DBUSER} <<EOF
SELECT '|' || schemaname || '|' || tablename || '|' || partitionrank || '|' || partitionrangestart || '|' || partitionrangeend || '|' || partitiontype
from pg_partitions where (tablename, partitionrank) in (SELECT tablename, max(partitionrank) from pg_partitions group by tablename)
;
EOF`
else
#select only named schemaname.tablename
all_partitions=`psql --tuples-only ${DBNAME} ${DBUSER} <<EOF
SELECT '|' || schemaname || '|' || tablename || '|' || partitionrank || '|' || partitionrangestart || '|' || partitionrangeend || '|' || partitiontype
from pg_partitions where (tablename, partitionrank) in (SELECT tablename, max(partitionrank) from pg_partitions group by tablename)
and tablename = '${tablename_parm}' and schemaname = '${schemaname_parm}'
;
EOF`
fi

# reset internal field separator to newline
IFS='
'

for i in $all_partitions 
do
schemaname=`echo $i | cut -d '|' -f 2`
tablename=`echo $i | cut -d '|' -f 3`
partitionrank=`echo $i | cut -d '|' -f 4`
partitionrangestart=`echo $i | cut -d '|' -f 5`
partitionrangeend=`echo $i | cut -d '|' -f 6`
partitiontype=`echo $i | cut -d '|' -f 7`

#echo
#echo $i
#echo $schemaname
#echo $tablename
#echo $partitionrank
#echo $partitionrangestart
#echo $partitionrangeend
#echo $partitiontype
echo "-- ###########"

# test to see if this is a range partition
if (echo ${partitiontype} | grep 'range' > /dev/null)
then
        echo "-- $schemaname.$tablename is range partitioned"
else
        echo "-- $schemaname.$tablename is not range partitioned, skipping"
        continue
fi


# Setup up standard format for date/time variable values: YYYY-MM-DD hh:mi:ss
# test to see if this is a date/timestamp interval
if (echo ${partitionrangestart} | grep 'timestamp' > /dev/null)
then
	echo "-- $schemaname.$tablename uses a timestamp interval"
	interval_type=timestamp
	# cut -d start-end removes timezone if it exists
        formatted_partitionrangestart=`echo $partitionrangestart | sed 's/::timestamp.*$//' | sed "s/'//g" | cut -c 1-19`
        formatted_partitionrangeend=`echo $partitionrangeend | sed 's/::timestamp.*$//' | sed "s/'//g" | cut -c 1-19`
elif (echo ${partitionrangestart} | grep 'date' > /dev/null)
then
	echo "-- $schemaname.$tablename uses a date interval"
	interval_type=date
	# need to add " 00:00:00" to date string
	formatted_partitionrangestart=`echo $partitionrangestart | sed 's/::date/ 00:00:00/' | sed "s/'//g"`
	formatted_partitionrangeend=`echo $partitionrangeend | sed 's/::date/ 00:00:00/' | sed "s/'//g"`
else
	echo "-- $schemaname.$tablename does not use a date/timestamp interval, skipping"
	continue
fi

echo "-- Current partitionrangestart = $formatted_partitionrangestart"
echo "-- Current partitionrangeend   = $formatted_partitionrangeend"


# convert dates into seconds (epoch) for comparisons
needed_end_date_seconds=`date "+%s" --date "${needed_end_date}"`
formatted_partitionrangeend_seconds=`date "+%s" --date "${formatted_partitionrangeend}"`

#echo ned_$needed_end_date
#echo fpe_$formatted_partitionrangeend
#echo neds_$needed_end_date_seconds
#echo fpes_$formatted_partitionrangeend_seconds

# If the needed end date is greater than the current partition end date create new partitions or else skip it
if [  ${needed_end_date_seconds} -gt ${formatted_partitionrangeend_seconds} ]
then
	echo "-- GOING AHEAD TO MAKE NEW PARTITONS"

#NOTE: at this point needed end date is greater then last partitions end date, loop through creating new partitions until
#      the last partitions end date is greater than or equal to the needed end date

# determine the time interval between the last partitions start and end dates
if [ ${interval_type} = "timestamp" ] || [ ${interval_type} = "date" ]
then
partition_interval=`psql --tuples-only ${DBNAME} ${DBUSER} <<EOF | sed 's/^ //' 
select ('$formatted_partitionrangeend'::timestamp - '$formatted_partitionrangestart'::timestamp);
EOF`

else
	echo "-- $schemaname.$tablename does not use a date/timestamp interval, skipping"
        continue
fi

# initialize variables with zero
days="0"
hours="0"
minutes="0"
seconds="0"
has_hours_flag=0

echo "-- partition_interval = ${partition_interval}"
# figure out what this partition_interval actually means
if ( echo ${partition_interval} | egrep '(day ..:..:..|days ..:..:..)' > /dev/null)
then
	echo "-- both day and hour:min:sec"
	days=`echo ${partition_interval} | cut -d ' ' -f 1`
	hours=`echo ${partition_interval} | cut -d ' ' -f 3 | cut -d ':' -f 1`
	minutes=`echo ${partition_interval} | cut -d ' ' -f 3 | cut -d ':' -f 2`
	seconds=`echo ${partition_interval} | cut -d ' ' -f 3 | cut -d ':' -f 3`
elif ( echo ${partition_interval} | grep day > /dev/null)
then
	echo "-- day part only, no hour:min:sec"
	# get first field, this is the number of days in the interval
	days=`echo ${partition_interval} | cut -d ' ' -f 1`
elif ( echo ${partition_interval} | grep '..:..:..' > /dev/null)
then
	echo "-- no day part, only hour:min:sec"
        hours=`echo ${partition_interval} | cut -d ':' -f 1`
        minutes=`echo ${partition_interval} | cut -d ':' -f 2`
        seconds=`echo ${partition_interval} | cut -d ':' -f 3`
fi

echo "-- dd = ${days}"
echo "-- hh = ${hours}"
echo "-- mm = ${minutes}"
echo "-- ss = ${seconds}"

# set flag to use later for naming hourly partitions
has_hours_flag=0
if [ ${hours} -gt 0 ]
then
	has_hours_flag=1
fi

# this is a month if the interval is 27, 28, 29, 30, 31
if (echo ${days} | egrep '(27|28|29|30|31)' > /dev/null)
then
	echo "-- $schemaname.$tablename interval is 1 month"
	create_partition_interval="1 month"
else
	create_partition_interval="${days} days ${hours} hours ${minutes} minutes"
	echo "-- $schemaname.$tablename interval is ${create_partition_interval}"
fi



# See if this table has a DEFAULT partition, if yes, do a split of last part. If no, add new part to end. 
has_default=`psql --tuples-only ${DBNAME} ${DBUSER} <<EOF | tr -d ' '
select count(*) as has_default from pg_partitions where partitionisdefault is true and schemaname = '${schemaname}' and tablename = '${tablename}';
EOF`


# initialize these before loop starts
new_start_date=${formatted_partitionrangestart}
new_end_date=${formatted_partitionrangeend}


# loop until the last partition is equal to or past the new needed end date
latest_partitionrangeend_seconds=${formatted_partitionrangeend_seconds}
while [ ${latest_partitionrangeend_seconds} -lt ${needed_end_date_seconds} ]
do

# get new START and END dates for new partition
if [ ${create_partition_interval} = "1 month" ]
then
new_start_date=`date "+%Y-%m-%d %T" --date="${new_start_date} 1 month"`
new_end_date=`date "+%Y-%m-%d %T" --date="${new_end_date} 1 month"`
else
new_start_date=$new_end_date
new_end_date=`date "+%Y-%m-%d %T" --date="${new_end_date}  ${create_partition_interval}"`
fi

#echo nsd__${new_start_date}
#echo ned__${new_end_date}

# convert latest partition end time into seconds (epoch) for test to end while loop
latest_partitionrangeend_seconds=`date "+%s" --date="${new_end_date}"`

# generate a new partition name from partition start date
# if the has_hours_flag is set to 1 then add the hour to the day name
if [ ${has_hours_flag} -eq 1 ]
then
	new_partition_name=`echo ${new_start_date} | cut -d ':' -f 1 | sed 's/-/_/g' |  sed 's/ /_/g'`
else
	new_partition_name=`echo ${new_start_date} | cut -d ' ' -f 1 | sed 's/-/_/g'`
fi


# create split partition command
if [ ${has_default} -eq 1 ]
then
#echo "-- $schemaname.$tablename has a default partition"
echo "
alter table ${schemaname}.${tablename} SPLIT default partition
START ('${new_start_date}') inclusive
END ('${new_end_date}') exclusive
into (partition p${new_partition_name}, default partition);
"
else
#echo "-- $schemaname.$tablename does NOT have a default partition"
echo "
alter table ${schemaname}.${tablename} ADD partition p${new_partition_name}
START ('${new_start_date}') inclusive
END ('${new_end_date}') exclusive;
"
fi

done # end of while loop

else
        echo "-- ${schemaname}.${tablename}: nothing to do: skipping"
        continue
fi

#done >> ${script_out} # end of for loop with redirect of output to sql script
done | tee -a ${script_out} # end of for loop with redirect of output to sql script

echo "rollback;" | tee -a ${script_out}
# echo "commit;" | tee -a ${script_out}

# Run script against db
#psql ${DBNAME} ${DBUSER} -f $script_out

# NOTE: need to add error checking when this is run from cron
