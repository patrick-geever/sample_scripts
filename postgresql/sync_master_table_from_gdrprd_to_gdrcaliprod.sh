#!/bin/bash

if [ ${#} -ne 1 ]
then
        echo
        echo "Sync master schema table from gdrprd to gdrcaliprod"
	echo
        echo "Usage: `basename $0` table"
        echo
        exit 1
fi

MAILFILE=/home/logs/sync_master_table_from_gdrprd_to_gdrcaliprod.mail
MAILLOG=/home/logs/sync_master_table_from_gdrprd_to_gdrcaliprod.log

# zero out maillog file
cp /dev/null $MAILFILE

## Source common variables
. /servers/config/gdr.env.sh

## Get script name and run tracker script
SCRIPT_NAME=`basename $0`; export SCRIPT_NAME
eval $TRACKER_SCRIPT $SCRIPT_NAME $TRACKER_NUMBER $TRACKER_HOSTNAME START

echo "Start: `date`"

table=$1

# Source EDB variables
. /opt/PostgresPlus/8.4AS/pgplus_env.sh

MAILLIST=GDROperations@company.com
#MAILLIST=pgeever@company.com
schema=master
error_flag=0
return_code=0

echo $schema.$table

# set this to see if command in psql has succeeded or failed
export ON_ERROR_STOP=1


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
	cat $MAILFILE >> $MAILLOG
        exit
}

# to catch an error in the copy pipeline, reports first non-zero return value, otherwise only last program in pipeline is caught
set -o pipefail

# First truncate table in target
psql -h gdrcaliproddb -p 5444 -d gdrcaliprod -U gdrcaliprod -c "truncate table ${schema}.${table};"
return_code=${?}
if [ ${return_code} -ne 0 ]; then error_message=`echo "truncate table ${schema}.${table} FAILED"`; Copy_Error; fi



# get number of rows to be copied out from source because copy will not report rows copied if to stdout
source_row_count=`psql -h ${DBHOSTNAME} -p ${DBPORT} -d ${DBNAME} -U ${DBUSER} -c "copy master.t_entity_desc to stdout;" | wc -l`
return_code=${?}
if [ ${return_code} -ne 0 ]; then error_message=`echo "source row count on ${schema}.${table} FAILED"`; Copy_Error; fi
echo "Source Row Count: $source_row_count"

# Copy data from source to target via pipeline
psql -h ${DBHOSTNAME} -p ${DBPORT} -d ${DBNAME} -U ${DBUSER} -c "copy ${schema}.${table} to stdout;" | psql -h gdrcaliproddb -p 5444 -d gdrcaliprod -U gdrcaliprod -c "copy ${schema}.${table} from stdin;" 
return_code=${?}
if [ ${return_code} -ne 0 ]; then error_message=`echo "copy of table ${schema}.${table} FAILED"`; Copy_Error; fi

target_row_count=`psql -h gdrcaliproddb -p 5444 -d gdrcaliprod -U gdrcaliprod -c "copy ${schema}.${table} to stdout;" | wc -l`
return_code=${?}
if [ ${return_code} -ne 0 ]; then error_message=`echo "target row count on ${schema}.${table} FAILED"`; Copy_Error; fi
echo "Target Row Count: $target_row_count"

# check that row counts match
if [ ${source_row_count} -ne ${target_row_count} ]
then
	error_message="Source Row Count: ${source_row_count} != Target Row Count: ${target_row_count}"
	Copy_Error
fi

echo "End: `date`"

## Run tracker script for end
eval $TRACKER_SCRIPT $SCRIPT_NAME $TRACKER_NUMBER $TRACKER_HOSTNAME END

cat $MAILFILE >> $MAILLOG

