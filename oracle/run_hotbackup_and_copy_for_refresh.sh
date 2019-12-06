#!/bin/bash

# source local .bash_profile
. ~/.bash_profile

export WorkDir=~/refresh_scripts

export TargetUserid=$1
export TargetOracleSid=$2

cd ${WorkDir}

sqlplus -s  "/ as sysdba" << EOF 

-- create backup script
spool hotbackup.commands.${ORACLE_SID}_to_${TargetOracleSid}.sql

set echo off
set feedback off
set linesize 2000
set trimspool on
set pagesize 5000
set heading off

SELECT command
FROM
  (select ts#, name, 1 as cmd_order, 'alter tablespace ' || name || ' begin backup;' as command
  from v\$tablespace
  where name not like 'TEMP%'
  UNION
  SELECT t.ts#, t.name, 2 AS cmd_order,
  '!nice -n 19 scp -c arcfour128 '||d.name ||' ${TargetUserid}@${TargetOracleSid}db:/oracle/${TargetOracleSid}/oradata1' as command
  FROM v\$tablespace t, v\$datafile d
  WHERE t.ts# = d.ts#
  UNION
  select ts#, name, 3 as cmd_order, 'alter tablespace ' || name || ' end backup;' as command
  from v\$tablespace
  where name not like 'TEMP%'
  --order by 1, 2 
  )
ORDER BY name, cmd_order, command;

prompt alter system archive log current;;

spool off

-- run backup script
@hotbackup.commands.${ORACLE_SID}_to_${TargetOracleSid}.sql

exit;
EOF



# copy archive logs to target
nice -n 19 scp /oracle/${ORACLE_SID}/arch/* ${TargetUserid}@${TargetOracleSid}db:/oracle/${TargetOracleSid}/arch


# get list of datafile names
sqlplus -s '/ as sysdba' << EOF | grep '[a-zA-Z0-9]' > datafile_names.${TargetOracleSid}.txt
set heading off
set feedback off
set trimspool on
set pagesize 1024
select substr(file_name, instr(file_name, '/', -1) +1 ) from dba_data_files;
exit;
EOF

# copy to target
scp datafile_names.${TargetOracleSid}.txt ${TargetUserid}@${TargetOracleSid}db:refresh_scripts/datafile_names.txt


