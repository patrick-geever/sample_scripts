#!/bin/bash

# 
# Move quarterly backup from regular s3 bucket to long term storage bucket
# Patrick Geever
# Dec 2017


#USAGE
function usage
{
  echo "Usage: `basename $0` <PG_CLUSTER>"
  exit 1;
}

if [ ${#} -ne 1 ]
then
  usage;
fi


vPgCluster=$1
export PGCLUSTER=$1

export PGENV_ASK=NO
. setenv_pg


function send_email {
    /usr/local/bin/mailDBA.ksh "$subjectTxt" $mailFile "dbajobs@company.com"
}


ERROR_FLAG=0

programName=`basename $0`
baseDir=${COMPANY_HOME}
logFile=${baseDir}/log/`basename $0 | cut -d. -f1`_$vPgCluster.log
mailFile=${baseDir}/log/`basename $0 | cut -d. -f1`_$vPgCluster.mail
S3_filelistFile=${baseDir}/log/`basename $0 | cut -d. -f1`_$vPgCluster.`date +%Y%m%d`.S3_filelist
vHostname=`hostname --fqdn`
vS3RemoteDir="s3://ps-s3.${vHostname}/${vPgCluster}/hotback"
vQuarterlyBucketDestination="s3://ps-s3.database.quarterly.backups"
PORT=5432
PGBIN_PATH=$PGHOME/bin
PSQL_BINARY=psql
DATABASE=postgres
BACKUPUSER=bkupmgr

AWSBIN_PATH=/home/enterprisedb/bin
AWS_BINARY=aws


# create date string name for this quarters backup
quarter_string=$( date +"%Y %m" | awk '{printf ("%4dq%1d\n", $1, ( ($2 - 1)/ 3 ) + 1 ) }' )


(
echo "START: `date`"

v_hotbackup_ok='FALSE'
v_pgdump_ok='FALSE'

# check whether cluster is in read/write or standby mode. If standby exit.
if $( ${PGBIN_PATH}/${PSQL_BINARY} -d ${DATABASE} -U ${BACKUPUSER} -p "$PORT" --tuples-only --command "select pg_is_in_recovery();" | grep t > /dev/null 2>&1 )
then
   echo "In Standby Mode"
   ERROR_FLAG=2
   subjectTxt="SKIPPED --> $programName $vPgCluster --  ${quarter_string} In Standby Mode"
else
  echo "Read/Write Mode: Proceeding"
fi


if [ "$ERROR_FLAG" -eq 0 ]
then
# check that the backup script logfiles exist and that the backups have finished successfully
if ( grep 'Backup Completed' ${COMPANY_HOME}/log/pg_backup_to_disk_${vPgCluster}.log > /dev/null )
then
   v_hotbackup_ok=TRUE
fi


if ( grep 'All dumps complete' ${COMPANY_HOME}/log/pgback.sh_${vPgCluster}.log > /dev/null )
then
   v_pgdump_ok=TRUE
fi


if [ ${v_hotbackup_ok} = 'TRUE' ] && [ ${v_pgdump_ok} = 'TRUE' ]
then
   echo "backup script logfiles ok"
   
   # get PG_BACKUP_SESSION_COMMON_ID value from backup log file
   PG_BACKUP_SESSION_COMMON_ID=$( grep PG_BACKUP_SESSION_COMMON_ID ${COMPANY_HOME}/log/pg_backup_to_disk_${vPgCluster}.log | cut -d '=' -f 2 | tr -d ' ' )
   echo "PG_BACKUP_SESSION_COMMON_ID = $PG_BACKUP_SESSION_COMMON_ID"
# NOTE: maybe get the value from both backup and pgdump logfiles and compare it to be sure both are the same

else
   echo "ERROR: Check backup script logfiles:"
   echo "${COMPANY_HOME}/log/pg_backup_to_disk_${vPgCluster}.log"
   echo "${COMPANY_HOME}/log/pgback.sh_${vPgCluster}.log"
   ERROR_FLAG=1
   subjectTxt="ERROR --> $programName $vPgCluster -- ${quarter_string} Check backup script logfiles"
fi
fi


if [ "$ERROR_FLAG" -eq 0 ]
then
# assemble file list from S3
S3_filelist=$( ${AWSBIN_PATH}/${AWS_BINARY} s3 ls ${vS3RemoteDir}/ | grep "${PG_BACKUP_SESSION_COMMON_ID}" | awk '{print $4}' | sort )
if [ $? -ne 0 ]
then
   ERROR_FLAG=1
   subjectTxt="ERROR --> $programName $vPgCluster -- ${quarter_string} s3 S3_filelist query failed"
fi
echo "${S3_filelist}"
fi


# Check tar file names
if [ "$ERROR_FLAG" -eq 0 ]
then
# Check that file system hotbackup filenames in the log file match the files at S3. If the files do not match exit with an error
v_s3_hotbackup_files_hash=`echo "${S3_filelist}" | egrep '(backup.*tar.gz$|backup.*tar$)' | sort | md5sum`
v_logfile_hotbackup_files_hash=$(for i in $(grep 'TARBALL' ${COMPANY_HOME}/log/pg_backup_to_disk_${vPgCluster}.log | cut -d '=' -f 2 | tr -d ' ' | sort); do basename $i; done | md5sum)


if [ "${v_s3_hotbackup_files_hash}" != "${v_logfile_hotbackup_files_hash}" ]
then
  echo "ERROR: file system hotbackup files checksum mismatch"
  echo "filenames from log and S3 do not match" 
   ERROR_FLAG=1
   subjectTxt="ERROR --> $programName $vPgCluster -- ${quarter_string} file system hotbackup files checksum mismatch"
else
  echo "hotbackup files match"
  echo "${v_s3_hotbackup_files_hash} = ${v_logfile_hotbackup_files_hash}" 
fi


fi


if [ "$ERROR_FLAG" -eq 0 ]
then
# Check that pgdump filenames in the log file match the files at S3. If the files do not match exit with an error
v_s3_pgdump_files_hash=`echo "${S3_filelist}" | grep pgdump | grep -v GLOBALS | sort | md5sum `
v_logfile_pgdump_files_hash=$(for i in $(grep 'Running pg_dump for database' ${COMPANY_HOME}/log/pgback.sh_${vPgCluster}.log | cut -d "=" -f 2 | sort); do basename $i; done | md5sum)


if [ "${v_s3_pgdump_files_hash}" != "${v_logfile_pgdump_files_hash}" ]
then
  echo "ERROR: pgdump files checksum mismatch"
  echo "filenames from log and S3 do not match" 
   ERROR_FLAG=1
   subjectTxt="ERROR --> $programName $vPgCluster -- ${quarter_string} pgdump files checksum mismatch"
else
  echo "pgdump files match"
  echo "$v_s3_pgdump_files_hash = $v_logfile_pgdump_files_hash"
fi
fi



if [ "$ERROR_FLAG" -eq 0 ]
then
if ( echo "${S3_filelist}" | grep $PG_BACKUP_SESSION_COMMON_ID | grep GLOBALS > /dev/null )
then
    echo "all files present"
else
   echo "pgdump GLOBALS file is missing"
   ERROR_FLAG=1
   subjectTxt="ERROR --> $programName $vPgCluster -- ${quarter_string} pgdump GLOBALS file is missing"
fi
fi


#if all files are present proceed with the move.

#pg NOTE: keep a list of files to be moved
if [ "$ERROR_FLAG" -eq 0 ]
then
echo "$S3_filelist" > ${S3_filelistFile}
fi

#######################################################################

# If all files were found then move the set of files to ps-s3.database.quarterly.backups for long term storage, 1 year
if [ "$ERROR_FLAG" -eq 0 ]
then

   echo "OK: Proceeding to move backup files to quarterly location"

   for i in `echo "$S3_filelist"`
   do

      # move file from regular to quarterly bucket
      ${AWSBIN_PATH}/${AWS_BINARY} s3 mv ${vS3RemoteDir}/${i}    ${vQuarterlyBucketDestination}/${vPgCluster}/${quarter_string}/${i} --quiet

      # Check for success of each move command
      if [ $? -eq 0 ]
      then
         echo "OK: ${AWSBIN_PATH}/${AWS_BINARY} s3 mv ${vS3RemoteDir}/${i}    ${vQuarterlyBucketDestination}/${vPgCluster}/${quarter_string}/${i} --quiet"
      else

             ########################
             # move retry
             # we had a move error, need to retry
             ERROR_FLAG=1

             for j in 1 2 3 4 5 6 7 8 9 10
             do
                  echo "Retry ${j}: backup piece ${i}"
                  ${AWSBIN_PATH}/${AWS_BINARY} s3 mv ${vS3RemoteDir}/${i}    ${vQuarterlyBucketDestination}/${vPgCluster}/${quarter_string}/${i} --quiet
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
                echo "OK: ${AWSBIN_PATH}/${AWS_BINARY} s3 mv ${vS3RemoteDir}/hotback/${i}    ${vQuarterlyBucketDestination}/${vPgCluster}/${quarter_string}/${i} --quiet"
              else
                 echo "ERROR: The backup piece file: ${i} move has failed at s3"
                 ERROR_FLAG=1
                 subjectTxt="ERROR --> $programName $vPgCluster -- ${quarter_string} The backup piece file: ${i} move has failed at s3"
              break
             fi
             ########################
      fi
   done
fi


#######################################################################

echo "END: `date`"


# send email 
if [ ${ERROR_FLAG} -eq 0 ]
then
   subjectTxt="SUCCESS --> $programName $vPgCluster -- ${quarter_string} backup has been moved to long term storage"
   rm -v ${S3_filelistFile}
   send_email
elif [ ${ERROR_FLAG} -eq 2 ]
then
   # In Standby Mode, Skipping.
   send_email
   cat $mailFile >> $logFile
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

