#!/bin/bash

set -x

if [[ $# < 1 ]]
then
   echo "\nUsage: `basename $0` <SID> \n\n"
   exit 1
fi

export ORAENV_ASK=NO
. /usr/local/bin/setenv_ora

programName=`basename $0`
vSid=$1
baseDir=${COMPANY_HOME}
logFile=${baseDir}/log/`basename $0 | cut -d. -f1`_$vSid.log
localDir=/u01/archive/${vSid}
#hostName=`hostname`
#dnsName=`nslookup $hostName | grep Name: | awk '{ print $2 }'`
remoteDir="s3://ps-s3.database.long.term/temp.co-lo.archlogs/${vSid}/"


( 

echo "
==========================================================
AWS Sync copy initiated `date`
==========================================================

Logging to $logFile

"


cd $localDir
# aws s3 sync $localDir $remoteDir --sse



#-----------------------------------------------------------------------------
# Setup script variables
#-----------------------------------------------------------------------------
# SCRIPTDIR=`dirname $0`
SCRIPTDIR=/u01/app/company/bin
SCRIPTNAME=`basename $0`
SCRIPTBASE=`echo $SCRIPTNAME | cut -d. -f1`
ENCRYPTIONPASSWORDFILE=${SCRIPTDIR}/gpg.passfile.txt


#pg filelist=$( comm -23 <(ls -l ${localDir}/ | awk '{print $9}' | sort)  <(aws s3 ls ${remoteDir} | awk '{print $4}' | sed 's/.gpg$//' | sort) | grep -v '^$' )

filelist=$( comm -23 <(find ${localDir}/ -type f -mmin -720 | rev | cut -d '/' -f1 | rev | sort)  <(aws s3 ls ${remoteDir} | awk '{print $4}' | sed 's/.gpg$//' | sort) | grep -v '^$' )

sleep 60

for filename in ${filelist}
do
    # Send file to S3, encrypt on the fly and store with ".gpg" extention at s3.
    # cat $filename | gzip --fast -c - | aws s3 cp --quiet - ${remoteDir}${filename}.gz --sse
     cat $filename | gpg --batch --passphrase-file ${ENCRYPTIONPASSWORDFILE} --symmetric --cipher-algo AES256 - | aws s3 cp --quiet - ${remoteDir}${filename}.gpg --sse
done

if [ $? -ne 0 ]; then
      echo "ERROR COPYING LOGS"
      exit 1
   else
      echo "Logs copied successfully"
fi

echo "

==========================================================
AWS Sync copy complete `date`
==========================================================
"

) > $logFile 2>&1

if [ $? -ne 0 ] ; then
   subjectTxt="ERROR --> $programName $vSid"
else
   subjectTxt="SUCCESS --> $programName $vSid"
fi

# /usr/local/bin/mailDBA.ksh "$subjectTxt" $logFile "dbajobs@company.com"
#cat $logFile | mail -s "$subjectTxt" "ctdba@company.com"
cat $logFile | mail -s "$subjectTxt" "pgeever@company.com"

