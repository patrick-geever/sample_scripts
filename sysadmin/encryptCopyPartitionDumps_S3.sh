#!/bin/bash

# 
# Encrypt and copy partition dump files to long term S3 storage bucket
# Patrick Geever
# July 2016


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


function send_email {
    /usr/local/bin/mailDBA.ksh "$subjectTxt" $mailFile "dbajobs@company.com"
}


ERROR_FLAG=0

programName=`basename $0`
baseDir=${COMPANY_HOME}
logFile=${baseDir}/log/`basename $0 | cut -d. -f1`_$vSid.log
mailFile=${baseDir}/log/`basename $0 | cut -d. -f1`_$vSid.mail

vBucket=s3.database.long.term
vPath=/u01/orabackup/export/$vSid/data_pump_dir
vPathMoveDir=/u01/orabackup/export/$vSid/data_pump_dir/partition_dumps_sent_to_S3
ENCRYPTIONPASSWORDFILE=${COMPANY_HOME}/bin/gpg.passfile.txt


(
echo "START: `date`"

# check for move dir
if [ -d ${vPathMoveDir} ]
then
   :
else
   mv ${vPathMoveDir} ${vPathMoveDir}.file
   mkdir ${vPathMoveDir}
fi

cd $vPath

# test for candiate files before proceding
vFileCount=`ls *.dmp *dmp.gz *log *.log.gz *log.txt 2> /dev/null | wc -l`
if [ ${vFileCount} -gt 0 ]
then 
   echo "Files present. Proceeding"
else
   echo "No files to process. Exiting"
   exit 0
fi



for i in `ls *dmp.gz *log *log.txt 2> /dev/null`
do
   cat $i | gpg --batch --passphrase-file ${ENCRYPTIONPASSWORDFILE} --symmetric --cipher-algo AES256 - | aws s3 cp - s3://${vBucket}${vPath}/${i}.gpg

   # if ok move file to partition_dumps_sent_to_S3
   if [ $? -eq 0 ]
   then 
      echo "OK: ${i} encrypted and moved to s3."
      mv -v ${i} partition_dumps_sent_to_S3
   else
      echo "ERROR: ${i} file issue"
      ERROR_FLAG=1
      subjectTxt="ERROR --> $programName $vSid -- File transfer problem to S3"
   fi
done


echo "END: `date`"


# send email 
if [ ${ERROR_FLAG} -eq 0 ]
then
   subjectTxt="SUCCESS --> $programName $vSid -- Partition Dumps has been moved to long term storage at S3"
   send_email
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

