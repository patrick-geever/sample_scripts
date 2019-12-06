#!/bin/bash

### Get script name and run tracker script
#TRACKER_SCRIPT=/servers/config/tracker_script.sh; export TRACKER_SCRIPT
#TRACKER_NUMBER=`date +%s`; export TRACKER_NUMBER
#TRACKER_HOSTNAME=`hostname`; export TRACKER_HOSTNAME
#
#SCRIPT_NAME=`basename $0`; export SCRIPT_NAME
#eval $TRACKER_SCRIPT $SCRIPT_NAME $TRACKER_NUMBER $TRACKER_HOSTNAME START

# get process id for logfiles
procpid=$$
OUTSIDE_MAILFILE=/home/logs/sync.gpfdist.captrans_track.gasaqa-to-insightdev.merge.${procpid}.mail


# create function to allow separate logfile per process
#pg function sync.gpfdist.captrans_track.merge
#pg {
echo "*##* START: `date`"
start_time_epoch=`date '+%s'`

inside_procpid=$1
MAILFILE=/home/logs/sync.gpfdist.captrans_track.gasaqa-to-insightdev.merge.${inside_procpid}.mail
LOGFILE=/home/logs/sync.gpfdist.captrans_track.gasaqa-to-insightdev.merge.log

#pg MAILLIST=dataservices@company.com
MAILLIST=pgeever@company.com

SCRIPTNAME=`basename $0`
LOCKFILE=/home/postgres/pjgeev/gpfdist_captrans.v3/lockfile.${SCRIPTNAME}

# remove lockfile after Control-C
trap "rm -vf ${LOCKFILE}; exit 1" SIGINT

# create lockfile.
# -l means: If older than 5400 seconds (90 min) remove file and proceed
# -s means: try for 2 seconds
# -r means: no retries
if (lockfile -l 5400 -s 2 -r 0 ${LOCKFILE})
then
        echo "Created lockfile: running script"
else
        echo "Script already running. Exiting"
        echo "${SCRIPTNAME} already running. Exiting! See: http://genapps.company.com/confluence/gpfdisk_captrans" | mail -s "${SCRIPTNAME} already running; Exiting!" $MAILLIST
        echo "#### END: `date` ##"
        cat $MAILFILE >> $LOGFILE
        rm -vf $MAILFILE
        exit 1
fi


# Source DB
DBHOSTNAME=localhost
DBPORT=5432
DBNAME=gasaqa
DBUSER=gasaqa

TARGET_DBHOSTNAME=insightdevdb
TARGET_DBPORT=5432
TARGET_DBNAME=insightdev
TARGET_DBUSER=insight


# source postgres variable
. /opt/PostgreSQL/9.0/pg_env.sh

dump_directory=/net/netapp2/dataservices/gpfdist/captrans_track_qa

master_server=''
primary_master=insightdevdb
secondary_master=insightdevdb
#pg primary_master=pdgreen01a
#pg secondary_master=pdgreen01b

# set this to see if command in psqlx has succeeded or failed
export ON_ERROR_STOP=1

# to catch an error in the copy pipeline, reports first non-zero return value, otherwise only last program in pipeline is caught
set -o pipefail

# initialize return_code
return_code=0

#email_function
function Notify 
{
       # send emails
       for addr in $MAILLIST
       do
          error_message="`basename ${0}` - ${error_message}"
          mail -s "ERROR: $error_message: ($MAILFLAG)" $addr < $MAILFILE
       done
}

function Copy_Error
{
        echo $return_code
        echo $error_message
        Notify
        cat $MAILFILE >> $LOGFILE
        rm -vf $MAILFILE
        rm -f ${LOCKFILE}
        exit
}


# create delete sql script function
##------------------------------------------------------------------------------
function mk_delete_script
{
modulus_num=50
counter=1
first_flag=0

echo '
\timing
begin;

'

while read line
do
if [ ${first_flag} -eq 0 ]
then
   echo "delete from maintenance.capacity_transaction where (location_role_id, eff_date, cycle_id) in (
 ($line)"
   first_flag=1
else
echo ",($line)"
fi

if [  $(( $counter % ${modulus_num} )) -eq 0 ]
then
   echo ");"
   echo
   first_flag=0
fi

((counter=$counter+1))
done < $1

# add last closing paren if required
closing_counter=$((counter=$counter - 1))

if [ $(( $closing_counter % ${modulus_num} )) -eq 0 ]
then
	:
else
	echo ");"
fi

echo
echo "commit;"
}
##------------------------------------------------------------------------------



# See if primary Greenplum master is up, if not use secondary
ping_check=`ping -c 5 ${primary_master} -w 10`
#pg echo "$ping_check"
pings_received=`echo "$ping_check" | grep received | cut -d ',' -f 2 | awk '{print $1}'`


# if pings_received is undefined set to 0
if [ -z ${pings_received} ]
then
	pings_received=0
fi


if [ ${pings_received} -gt 0 ]
then
	echo "Primary is Master: ${primary_master}"
	master_server=${primary_master}
else
	echo "Secondary is Master: ${secondary_master}"
	master_server=${secondary_master}
fi


####################
# ack new committed rows in tracking table

temp_tablename=captrans_gpfdist_temp_${procpid}

echo "Ack Committed Rows"
ack_command_output=`psql -h ${DBHOSTNAME} -p ${DBPORT} -d ${DBNAME} -U ${DBUSER} <<EOF

\timing

begin;

-- get list of rows where acknowledged is null
create temporary table ${temp_tablename} as 
SELECT location_role_id, eff_date, cycle_id, newest_row from maintenance.capacity_transaction_track where newest_row_committed is null order by newest_row;


update maintenance.capacity_transaction_track set newest_row_committed = nextval('maintenance.capacity_transaction_track_newest_row_committed_seq'), newest_row_committed_timestamp = now() 
where (location_role_id, eff_date, cycle_id, newest_row) in (select location_role_id, eff_date, cycle_id, newest_row from ${temp_tablename});

drop table ${temp_tablename};

-- run commit, test for error or rollback in output
commit;
\q
EOF`

echo "$ack_command_output"
if (echo "$ack_command_output" | egrep '(ERROR|ROLLBACK)')
then
   error_message="Ack in ${DBNAME} DB FAILED"
   Copy_Error
fi

####################


# get end newest_row_committed from last batch
last_row_replicated=`psqlx -h ${DBHOSTNAME} -p ${DBPORT} -d ${DBNAME} -U ${DBUSER} --tuples-only --quiet  <<EOF | sed 's/^  //' | sed 's/^ //' | sed '$d' 
select max(last_row_replicated) from maintenance.capacity_transaction_track_replication where target_tablename = 'maintenance.capacity_transaction';
\q
EOF`
return_code=${?}
if [ ${return_code} -ne 0 ]; then error_message=`echo "connection to target db FAILED"`; Copy_Error; fi

echo "last_row_replicated = $last_row_replicated"


# get latest newest_row_committed from source
source_newest_row_committed=`psqlx -h ${DBHOSTNAME} -p ${DBPORT} -d ${DBNAME} -U ${DBUSER} --tuples-only --quiet  <<EOF | sed 's/^  //' | sed 's/^ //' | sed '$d' 
select max(newest_row_committed) from maintenance.capacity_transaction_track;
\q
EOF`
return_code=${?}
if [ ${return_code} -ne 0 ]; then error_message=`echo "connection to source db FAILED"`; Copy_Error; fi

echo "source_newest_row_committed = $source_newest_row_committed"


# check to see if any new rows have been created in source
if [ ${source_newest_row_committed} -gt ${last_row_replicated} ]
then
	:
else
        echo "No new rows. Exiting"
        echo "#### END: `date`"
        # NOTE: Normal exit so run end tracker
        cat $MAILFILE >> $LOGFILE
        rm -vf $MAILFILE
        rm -f ${LOCKFILE}
        exit
fi

# get high and low number for archive file name
(( low_number = $last_row_replicated + 1 ))
high_number=$source_newest_row_committed

echo "low_number = $low_number -- high_number = $high_number"

dump_file=captrans_track.`date '+%Y%m%d_%H%M%S'`.${low_number}.${high_number}.merge.dmp
dump_file_delete=captrans_track.`date '+%Y%m%d_%H%M%S'`.${low_number}.${high_number}.merge.delete.dmp
delete_script_file=captrans_track.`date '+%Y%m%d_%H%M%S'`.${low_number}.${high_number}.merge.delete.sql

# Check to see if there are any deletes in this batch that we care about, ie. the 'D' row is the last one in the batch for a particular key
delete_flag=`psqlx -h ${DBHOSTNAME} -p ${DBPORT} -d ${DBNAME} -U ${DBUSER} --tuples-only --quiet  <<EOF | sed 's/^  //' | sed 's/^ //' | sed '$d'
select count(*) from (
select id, location_role_id, eff_date, cycle_id, operational_cap, available_cap, scheduled_cap, design_cap, interruptible_cap, interruptible_ind, measurement_basis_id, end_date, post_date, fetch_date, insert_date, post_date_type, gas_day, job_queue_id, status, row_number() over (partition by location_role_id, eff_date, cycle_id order by newest_row_committed desc) as gpload_row_number 
from maintenance.capacity_transaction_track 
where newest_row_committed > ${last_row_replicated} and newest_row_committed <= ${source_newest_row_committed} order by newest_row_committed
) as x where gpload_row_number = 1 and status = 'D';
\q
EOF`

if [ ${delete_flag} -gt 0 ]
then
   echo "deletes are present"
   # deal with deletes
   #1 select key (location_role_id, eff_date, cycle_id) of the newest row for each key that are deletes into a delete dump file to be used to generate delete script

   time psqlx -h ${DBHOSTNAME} -p ${DBPORT} -d ${DBNAME} -U ${DBUSER} -c "\copy (select location_role_id || ',''' || eff_date || ''',' || cycle_id || '' from (
select id, location_role_id, eff_date, cycle_id, operational_cap, available_cap, scheduled_cap, design_cap, interruptible_cap, interruptible_ind, measurement_basis_id, end_date, post_date, fetch_date, insert_date, post_date_type, gas_day, job_queue_id, status, row_number() over (partition by location_role_id, eff_date, cycle_id order by newest_row_committed desc) as gpload_row_number 
from maintenance.capacity_transaction_track 
where newest_row_committed > ${last_row_replicated} and newest_row_committed <= ${source_newest_row_committed} order by newest_row_committed) as x where gpload_row_number = 1 and status = 'D') to ${dump_directory}/${dump_file_delete}"
   return_code=${?}
   if [ ${return_code} -ne 0 ]; then error_message=`echo "Copy command from source db FAILED"`; Copy_Error; fi

   #2 select * for the newest row for each key that are not deletes into the usual copy dump file.

   time psqlx -h ${DBHOSTNAME} -p ${DBPORT} -d ${DBNAME} -U ${DBUSER} -c "\copy (select id, location_role_id, eff_date, cycle_id, operational_cap, available_cap, scheduled_cap, design_cap, interruptible_cap, interruptible_ind, measurement_basis_id, end_date, post_date, fetch_date, insert_date, post_date_type, gas_day, job_queue_id from (
select id, location_role_id, eff_date, cycle_id, operational_cap, available_cap, scheduled_cap, design_cap, interruptible_cap, interruptible_ind, measurement_basis_id, end_date, post_date, fetch_date, insert_date, post_date_type, gas_day, job_queue_id, status, row_number() over (partition by location_role_id, eff_date, cycle_id order by newest_row_committed desc) as gpload_row_number 
from maintenance.capacity_transaction_track 
where newest_row_committed > ${last_row_replicated} and newest_row_committed <= ${source_newest_row_committed} order by newest_row_committed) as x where gpload_row_number = 1 and status != 'D') to ${dump_directory}/${dump_file} delimiter '|'"
   return_code=${?}
   if [ ${return_code} -ne 0 ]; then error_message=`echo "Copy command from source db FAILED"`; Copy_Error; fi

else

   echo "Copy timing:"
   # No deletes: Copy out all newest rows for each key since last batch
   time psqlx -h ${DBHOSTNAME} -p ${DBPORT} -d ${DBNAME} -U ${DBUSER} -c "\copy (select id, location_role_id, eff_date, cycle_id, operational_cap, available_cap, scheduled_cap, design_cap, interruptible_cap, interruptible_ind, measurement_basis_id, end_date, post_date, fetch_date, insert_date, post_date_type, gas_day, job_queue_id from (
select id, location_role_id, eff_date, cycle_id, operational_cap, available_cap, scheduled_cap, design_cap, interruptible_cap, interruptible_ind, measurement_basis_id, end_date, post_date, fetch_date, insert_date, post_date_type, gas_day, job_queue_id, status, row_number() over (partition by location_role_id, eff_date, cycle_id order by newest_row_committed desc) as gpload_row_number 
from maintenance.capacity_transaction_track 
where newest_row_committed > ${last_row_replicated} and newest_row_committed <= ${source_newest_row_committed} order by newest_row_committed) as x where gpload_row_number = 1 and status != 'D') to ${dump_directory}/${dump_file} delimiter '|'"
   return_code=${?}
   if [ ${return_code} -ne 0 ]; then error_message=`echo "Copy command from source db FAILED"`; Copy_Error; fi

fi

# if we have dump files with EITHER insert/updates or deletes or BOTH we need to proceed.
# Check if files are zero length. We should have exited before here if there is no new data
if [ ${delete_flag} -eq 0 ]
then
	if [ ! -s ${dump_directory}/${dump_file} ]; then error_message=`echo "Dumpfile is zero length"`; Copy_Error; fi
elif [ ${delete_flag} -gt 0 ]
then
	if [ ! -s ${dump_directory}/${dump_file} ] && [ ! -s ${dump_directory}/${dump_file_delete} ]; then error_message=`echo "Dumpfiles are zero length"`; Copy_Error; fi
fi

# if delete_flag is -gt 0 create and run delete script here
if [ ${delete_flag} -gt 0 ]
then

  # get row count from delete dumpfile
  rowsindumpfiledelete=`wc -l ${dump_directory}/${dump_file_delete} | cut -d ' ' -f 1`
  echo "Rows in dumpfiledelete = $rowsindumpfiledelete"

  # create delete sql script file
  mk_delete_script  ${dump_directory}/${dump_file_delete} > ${dump_directory}/${delete_script_file}

  # log output of the delete and look for errors in case this rolls back
  echo "Running deletes"
  time delete_command_output=$(psqlx -h ${TARGET_DBHOSTNAME} -p ${TARGET_DBPORT} -d ${TARGET_DBNAME} -U ${TARGET_DBUSER} -f  ${dump_directory}/${delete_script_file} 2>&1)
  echo "$delete_command_output"
  if (echo "$delete_command_output" | egrep '(ERROR|ROLLBACK)')
  then 
     error_message="Delete script in ${TARGET_DBNAME} DB FAILED"
     Copy_Error
  fi

fi

# get row count from dumpfile
rowsindumpfile=`wc -l ${dump_directory}/${dump_file} | cut -d ' ' -f 1`
echo "Rows in dumpfile = $rowsindumpfile"


# make symbolic link to latest file for gpload
if [ -e ${dump_directory}/captrans_track.merge.dmp ]; then rm -fv ${dump_directory}/captrans_track.merge.dmp; fi
ln -s  ${dump_directory}/${dump_file}  ${dump_directory}/captrans_track.merge.dmp


# get hostname of master db server in case of failover to pick right loader config file
if [ ${master_server} = ${primary_master} ]
then
        master_hostname=mdw
else
        master_hostname=smdw
fi

echo "master_hostname = $master_hostname"



# run gpload command, set env variables first
gpload_output=`. /usr/local/greenplum-loaders-4.2.3.0-build-1/greenplum_loaders_path.sh; gpload -f /usr/local/company/merge.captrans_merge.${master_hostname}.yaml`

echo "$gpload_output"

# check to see if loader succeeded.
# grep for string "|INFO|gpload succeeded"
if ( echo "$gpload_output" | grep 'gpload succeeded' > /dev/null 2>&1 )
then
	echo 'gpload succeeded'
else
	error_message=`echo "gpload for target table FAILED"`
	Copy_Error;
fi

end_time_epoch=`date '+%s'`
(( batch_elapsed_time = $end_time_epoch - $start_time_epoch ))
echo batch_elapsed_time = $batch_elapsed_time

# update track replication table to add last_row_replicated 
track_replication_update_output=`psqlx -e -h ${DBHOSTNAME} -p ${DBPORT} -d ${DBNAME} -U ${DBUSER} --tuples-only  <<EOF | sed 's/^  //' | sed 's/^ //' | sed '$d'
insert into maintenance.capacity_transaction_track_replication (source_tablename, target_tablename, last_row_replicated, last_row_replicated_timestamp, batch_elapsed_time) 
values ('maintenance.capacity_transaction', 'maintenance.capacity_transaction', ${high_number}, now(), ${batch_elapsed_time} * interval '1 second');
\q
EOF`
return_code=${?}
if [ ${return_code} -ne 0 ]; then error_message=`echo "FAILED: insert into maintenance.capacity_transaction_track_replication (source_tablename, target_tablename, last_row_replicated, last_row_replicated_timestamp) values ('maintenance.capacity_transaction', 'maintenance.capacity_transaction', ${high_number}, now());`; Copy_Error; fi

echo "track_replication_update_output = $track_replication_update_output"


# get total of updates and inserts to check matching number of rows in file
# get inserts
inserts=`echo "$gpload_output" | grep Inserted | tr -s ' ' | cut -d '=' -f 2`
echo "Inserts = $inserts"

# get updates
updates=`echo "$gpload_output" | grep Updated | tr -s ' ' | cut -d '=' -f 2`
echo "Updates = $updates"

(( total_inserts_updates = $inserts +  $updates ))
echo "total_inserts_updates = $total_inserts_updates"

(( batch_range = $high_number - $last_row_replicated ))
echo "          batch_range = $batch_range"

if [ ${rowsindumpfile} -ne ${total_inserts_updates} ]
then
	echo "Rows in Dumpfile do not match Total Inserts and Updates"
fi


rm -v ${dump_directory}/captrans_track.merge.dmp
mv -v ${dump_directory}/${dump_file}  ${dump_directory}/archive
return_code=${?}
if [ ${return_code} -ne 0 ]; then error_message=`echo "move and/or rename of dumpfile FAILED"`; Copy_Error; fi

if [ ${delete_flag} -gt 0 ]
then
   mv -v ${dump_directory}/${dump_file_delete} ${dump_directory}/archive
   mv -v ${dump_directory}/${delete_script_file} ${dump_directory}/archive
fi

rm -f ${LOCKFILE}

echo "#### END: `date`"

cat $MAILFILE >> $LOGFILE

#pg } # end function 


# call function, pass in procpid, direct stdout and stderr to MAILFILE
#pg sync.gpfdist.captrans_track.merge $procpid > $OUTSIDE_MAILFILE 2>&1

rm -f $OUTSIDE_MAILFILE


## Run tracker script for end
#eval $TRACKER_SCRIPT $SCRIPT_NAME $TRACKER_NUMBER $TRACKER_HOSTNAME END



