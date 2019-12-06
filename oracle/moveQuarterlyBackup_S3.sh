#!/bin/bash

# 
# Move quarterly backup from regular s3 bucket to long term storage bucket
# Patrick Geever
# May 2016


#USAGE
function usage
{
  echo "Usage: `basename $0` <SID>"
  exit 1;
}

if [ ${#} -ne 1 ]
then
  usage;
fi


vSid=$1
vSidUpperCase=`echo ${vSid} | tr '[a-z]' '[A-Z]'`
vHostname=`hostname --fqdn`


function send_email {
    /usr/local/bin/mailDBA.ksh "$subjectTxt" $mailFile "dbajobs@company.com"
}


ERROR_FLAG=0

programName=`basename $0`
baseDir=${COMPANY_HOME}
logFile=${baseDir}/log/`basename $0 | cut -d. -f1`_$vSid.log
mailFile=${baseDir}/log/`basename $0 | cut -d. -f1`_$vSid.mail
filelistFile=${baseDir}/log/`basename $0 | cut -d. -f1`_$vSid.`date +%Y%m%d`.filelist
vQuarterlyBucketDestination=s3.database.quarterly.backups
export ORACLE_SID=${vSid}
export ORAENV_ASK=NO
. /usr/local/bin/setenv_ora

# create date string name for this quarters backup
quarter_string=$( date +"%Y %m" | awk '{printf ("%4dq%1d\n", $1, ( ($2 - 1)/ 3 ) + 1 ) }' )



(
echo "START: `date`"
export NLS_DATE_FORMAT='YYYY-MM-DD:HH24:MI:SS'

# Get listing of backup pieces for the latest completed backup for database
filelist=`sqlplus -s rman_xxxxxxxx/xxxxxxxx@oem01.company.com <<EOF
set heading off
set linesize 200
set pagesize 2000
set feedback off
SET NEWPAGE NONE

select 
SUBSTR(handle, INSTR(handle,'/', -1) + 1 )
from RC_BACKUP_PIECE_DETAILS where bs_key in (
select bs_key from rc_backup_set_details where session_key = (
  select session_key from RC_RMAN_BACKUP_JOB_DETAILS where db_key = ( select db_key from RC_SITE where DB_UNIQUE_NAME = '\$vSidUpperCase')
  and end_time = (
    select max(j.end_time) from RC_RMAN_BACKUP_JOB_DETAILS j, RC_BACKUP_SET_DETAILS s
      where j.end_time is not null 
      and j.status like 'COMPLETED%'
      and j.input_type = 'DB INCR'
      and j.db_key = s.db_KEY
      and j.session_key = s.SESSION_KEY
      and ((s.INCREMENTAL_LEVEL = 0 and s.backup_type = 'I') or s.backup_type = 'D')
      and j.db_key = (
          select db_key from RC_SITE where DB_UNIQUE_NAME = '\$vSidUpperCase'
      )
    )
  )
)
order by completion_time;
exit;
EOF`

if [ $? -eq 0 ]
then 
   echo "OK: Catalog query succeeded"
else
   echo "ERROR: Catalog query failed"
   ERROR_FLAG=1
   subjectTxt="ERROR --> $programName $vSid -- Catalog query failed"
fi

#pg NOTE: keep a list of files to be moved
echo "$filelist" > ${filelistFile}

# See if the files exist. If any file is missing exit with an error
if [ "$ERROR_FLAG" -eq 0 ]
then
   for i in `echo "$filelist"`
   do
      aws s3 ls s3://s3.${vHostname}/${vSid}/hotback/${i}
      if [ $? -eq 0 ]
      then 
         echo "OK: ${i} exists at s3"
      else
         echo "ERROR: The backup piece file: ${i} is missing at s3"
         ERROR_FLAG=1
         subjectTxt="ERROR --> $programName $vSid -- The backup piece file: ${i} is missing at s3"
         break
      fi
   done
fi

# If all files were found then move the set of files to s3.database.quarterly.backups for long term storage, 1 year
if [ "$ERROR_FLAG" -eq 0 ]
then

   echo "OK: Proceeding to move backup files to quarterly location"

   for i in `echo "$filelist"`
   do

      # move file from regular to quarterly bucket
      aws s3 mv s3://s3.${vHostname}/${vSid}/hotback/${i}    s3://${vQuarterlyBucketDestination}/${vSid}/${quarter_string}/${i} --quiet

      # Check for success of each move command
      if [ $? -eq 0 ]
      then
         echo "OK: aws s3 mv s3://s3.${vHostname}/${vSid}/hotback/${i}    s3://${vQuarterlyBucketDestination}/${vSid}/${quarter_string}/${i} --quiet"
      else

             ########################
             # move retry
             # we had a move error, need to retry
             ERROR_FLAG=1

             for j in 1 2 3 4 5 6 7 8 9 10
             do
                  echo "Retry ${j}: backup piece ${i}"
                  aws s3 mv s3://s3.${vHostname}/${vSid}/hotback/${i}    s3://${vQuarterlyBucketDestination}/${vSid}/${quarter_string}/${i} --quiet
                  if [ $? -ne 0 ] && [ ${j} -le 10 ]
                  then
                     ERROR_FLAG=0
                     echo ok_${j}
                     break
                  else
                     echo "trying again -- continue loop after sleep"
                     echo bad_${j}
                     sleep 60
                   fi
             done

             if [ "$ERROR_FLAG" -eq 0 ]
             then
                echo "OK: aws s3 mv s3://s3.${vHostname}/${vSid}/hotback/${i}    s3://${vQuarterlyBucketDestination}/${vSid}/${quarter_string}/${i} --quiet"
              else
                 echo "ERROR: The backup piece file: ${i} move has failed at s3"
                 ERROR_FLAG=1
                 subjectTxt="ERROR --> $programName $vSid -- The backup piece file: ${i} move has failed at s3"
              break
             fi
             ########################
      fi
   done
fi


echo "END: `date`"


# send email 
if [ ${ERROR_FLAG} -eq 0 ]
then
   subjectTxt="SUCCESS --> $programName $vSid -- ${quarter_string} backup has been moved to long term storage"
   send_email
   rm -v ${filelistFile}
else
   # subjectTxt is set above by file missing or move fail loops
   send_email
   cat $mailFile >> $logFile
   exit 1
fi


) > $mailFile 2>&1

if [ $? -ne 0 ]
then
   exit 1
fi

cat $mailFile >> $logFile

