#!/bin/bash

ulimit -f unlimited
TEMP=/tmp
TMPDIR=/tmp
BASH_ENV=~/.bash_profile
LOCAL_DIR=/usr/local/company
ORACLE_HOME=/opt/oracle/product/server64/9.2.0
LD_LIBRARY_PATH=$ORACLE_HOME/lib:$ORACLE_HOME/lib32:/lib64:/lib:/usr/lib64:/usr/lib
PATH=$PATH:$ORACLE_HOME/bin:/sbin
ORACLE_BASE=/opt/oracle
ORACLE_SID=prod
EDITOR=vi
export TEMP TMPDIR BASH_ENV ORACLE_HOME ORACLE_BASE ORACLE_SID EDITOR PATH LD_LIBRARY_PATH LOCAL_DIR
umask 022
ERROR_FLAG=0
mail_list="adminlogs@adminlogs.company.com pgeever@company.com"
error_mail_list="adminlogs@adminlogs.company.com pgeever@company.com"

MSG=
SYS=`uname -n`
SID=$ORACLE_SID
ProgramName=`basename $0`
MAIL=/home/logs/`echo $ProgramName | sed -e 's/\.sh$/\.mail/'`
LOG=/home/logs/`echo $ProgramName | sed -e 's/\.sh$/\.log/'`

function check_error_set_flag
{
if [ ${RETURN_CODE} -ne 0 ]
then
        ERROR_FLAG=1
        echo "ERROR = $ERROR"
fi
}

function Notify 
{
   if [ ${ERROR_FLAG} -eq 0 ]
   then
      for addr in $mail_list
      do
          mail -s "$MSG : $ProgramName : $SYS : $SID" $addr < $MAIL
      done
   else
       for addr in $error_mail_list
       do
          mail -s "$MSG : $ProgramName : $SYS : $SID" $addr < $MAIL
       done
    fi
}


echo "START: `date`" > $MAIL

HotDir=/alpha/hotbackups/prod
cd /home/oracle/prod/hotbackup

# remove old backup
rm ${HotDir}/*
RETURN_CODE=$?
ERROR=remove_old_backup
check_error_set_flag

sqlplus "/ as sysdba" @${LOCAL_DIR}/report_backup_status.sql
RETURN_CODE=$?
ERROR=report_backup_status.sql
check_error_set_flag

sqlplus "/ as sysdba" @${LOCAL_DIR}/HotBackup.single.sql $HotDir
RETURN_CODE=$?
ERROR=HotBackup.single.sql
check_error_set_flag

Found=0
for i in `ls -1t /opt/oracle/admin/prod/udump/prod* | head`
do
 cat $i | grep 'CREATE CONTROLFILE' >/dev/null 2>&1
 if [ $? -eq 0 -a $Found -eq 0 ]; then
   cp $i create_controlfile.sql
   cp create_controlfile.sql $HotDir
   RETURN_CODE=$?
   ERROR=create_controlfile.sql
   check_error_set_flag
   Found=1
 fi
done

cd ${ORACLE_HOME}/dbs 
cp initprod.ora $HotDir 
RETURN_CODE=$?
ERROR=initprod.ora
check_error_set_flag

cp spfileprod.ora $HotDir 
RETURN_CODE=$?
ERROR=spfileprod.ora
check_error_set_flag

Count=`sqlplus -s "/ as sysdba" @${LOCAL_DIR}/check_backup_status.sql | wc -l`
RETURN_CODE=$?
ERROR=check_backup_status.sql.2
check_error_set_flag

if [ $Count -ne 0 ]; then
   BackupInfo=`sqlplus "/ as sysdba" @${LOCAL_DIR}/check_backup_status.sql`
   RETURN_CODE=$?
   ERROR=check_backup_status.sql.3
   check_error_set_flag

   sqlplus "/ as sysdba" @${LOCAL_DIR}/fix_backup_status.sql
   RETURN_CODE=$?
   ERROR=fix_backup_status.sql
   check_error_set_flag
fi

sqlplus "/ as sysdba" @${LOCAL_DIR}/report_backup_status.sql
RETURN_CODE=$?
ERROR=report_backup_status.sql.2
check_error_set_flag

if [ ${ERROR_FLAG} -eq 0 ]
then
	MSG="SUCCESS"
	Notify
else
	MSG="ERROR"
	Notify
fi


# get list of datafile names
sqlplus -s '/ as sysdba' << EOF | grep '[a-zA-Z0-9]' > ${HotDir}/datafile_names.txt
set heading off
set feedback off
set trimspool on
set pagesize 1024
select substr(file_name, instr(file_name, '/', -1) +1 ) from dba_data_files;
exit;
EOF

# Copy latest backup to electra2
/usr/bin/rsh -l oracle rac2 "/bin/rm /alpha/net_backups/hotbackups/prod/*"
tar -C ${HotDir} -cvf - . | /usr/bin/rsh rac2 "cd /alpha/net_backups/hotbackups/prod; tar xf -"


echo "END: `date`"

cat $MAIL >> $LOG
