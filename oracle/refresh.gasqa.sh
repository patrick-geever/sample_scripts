#!/bin/bash

export ORACLE_SID=gasqa
export ORACLE_HOME=/opt/oracle/product/server64/9.2.0
export BackupDir=/alpha/net_backups/hotbackups/gasprod
export WorkDir=~/${ORACLE_SID}
export ArchDir=/alpha/arch/${ORACLE_SID}
export RedoDir=/alpha/redo/${ORACLE_SID}
export DbDir=/alpha/oradata/${ORACLE_SID}
export SedPath="\/alpha\/oradata\/${ORACLE_SID}\/"


echo 
echo "Be sure to disable the snapshot job on hades first."
echo
echo "Hit Enter to continue"
read


# run hotbackup on prod host
ssh oracle@gasdb1 /usr/local/company/HotBackup_GASPROD.single.sh


# kill db
sqlplus "/ as sysdba" << EOF
shutdown abort;
exit;
EOF

# remove old db
rm -v ${ArchDir}/*.arc
rm -v ${RedoDir}/*
rm -v ${DbDir}/*


# copy files into place
cp -v ${BackupDir}/* ${DbDir}
cp -v ${BackupDir}/datafile_names.txt ${WorkDir}
cp -v ${BackupDir}/*.arc ${ArchDir}


# rename archive logs
cd ${ArchDir}
./fix_filenames.sh


# create new create_controlfile.sql file
cd ${WorkDir}

# add path to filenames
cat datafile_names.txt | sed -e "s/^/\'${SedPath}/" | sed -e "s/$/\'/" |  sed -e "\$q;s/$/,/g" > path_datafile_names.txt

# create controlfile script
cat create_controlfile.rename_to_${ORACLE_SID}.1.sql > create_controlfile.rename_to_${ORACLE_SID}.sql
cat path_datafile_names.txt >> create_controlfile.rename_to_${ORACLE_SID}.sql
cat create_controlfile.rename_to_${ORACLE_SID}.2.sql >> create_controlfile.rename_to_${ORACLE_SID}.sql

sqlplus '/ as sysdba' << EOF
@create_controlfile.rename_to_${ORACLE_SID}.sql
exit;
EOF


# Change DBID
sqlplus '/ as sysdba' << EOF
set echo on
set verify on 
shutdown immediate
startup mount
exit;
EOF

echo Y | nid target=sys/dbabofh

sqlplus '/ as sysdba' << EOF
shutdown immediate
!rm ${ORACLE_HOME}/dbs/orapw${ORACLE_SID}
!orapwd file=${ORACLE_HOME}/dbs/orapw${ORACLE_SID} password=dbabofh entries=10
startup mount
alter database open resetlogs;
select dbid,name,open_mode,activation#,created from v\$database;
exit;
EOF


cd ${WorkDir}/Fix_Production_Copy
sqlplus '/ as sysdba' << EOF
@chgpasswd.sql
exit;
EOF

${WorkDir}/Fix_Production_Copy/Fix_Production_Copy.sh

echo "$ORACLE_SID has been refreshed on `hostname`" | mail -s "$ORACLE_SID has been refreshed on `hostname`" pgeever@company.com

