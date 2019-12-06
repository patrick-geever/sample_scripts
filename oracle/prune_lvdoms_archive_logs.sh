#!/bin/bash
########################################################################
#   Filename   :  prune_lvdoms_archive_logs.sh
#
#   Author     :  Patrick Geever
#
#   Purpose    :  This script moves removes old archive logs more that
#                 14 days old.
#
#   Date         Who            What
#
#   12/06/01     Patrick G.     Created
########################################################################

# source .profile
. ~/.profile
PATH=/usr/bin:$PATH; export PATH
umask 002
unalias rm

DAYS_OLD=14
BACKUP_ARCHIVE_LOG_DIRECTORY="/home0/oraarch/lvdoms"
ARCHIVE_LOG_PREFIX=lvdoms_arch1_
FILELIST=""
MAIL_FILELIST=""
ERROR_FLAG=0
mail_list="dba_staff@company.com"

cd $BACKUP_ARCHIVE_LOG_DIRECTORY


#-----------------------------------------------------------------
function Notify
{
   for addr in $mail_list
   do
        if [ "${ERROR_FLAG}" -eq 0 ]
        then
        echo $FILELIST | mailx -s "Old Files: ${MAIL_FILELIST} Successfully removed from ${BACKUP_ARCHIVE_LOG_DIRECTORY} on `hostname`" $addr 
        else
        echo $FILELIST | mailx -s "ERROR: Problem removing ${MAIL_FILELIST} from $BACKUP_ARCHIVE_LOG_DIRECTORY on `hostname`" $addr 
        fi
   done
}
#-----------------------------------------------------------------

# get filenames of all files which should be removed
FILELIST=`find . -mtime +${DAYS_OLD} -name "${ARCHIVE_LOG_PREFIX}*" `

# check for no files found. If no files found just exit
if [ "${FILELIST}" ]
then
        :
else
        exit
fi      


# remove newlines from filelist, makes mail subject line hard to read
MAIL_FILELIST=`echo "$FILELIST" | tr '\n' ' ' `

# remove the files
if (rm $FILELIST)
then
        :
else
        ERROR_FLAG=1
fi

Notify

