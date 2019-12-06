#!/bin/bash

echo "START: `date`"

export PG_HOME=/opt/PostgreSQL/9.1
export REMOTE_DBNAME=agqa
export REMOTE_SYSTEM=${REMOTE_DBNAME}db
export LOCAL_DBNAME=agprod
export LOCAL_SYSTEM=${LOCAL_DBNAME}db
export REMOTE_FILE_SYS_ROOT=/alpha/postgres-${REMOTE_DBNAME}
export LOCAL_FILE_SYS_ROOT=/alpha/postgres-${LOCAL_DBNAME}
export PSQL_EXEC=psql
export LOG_DIR=pg_log
export BASE_DB=postgres
export PATH=$PG_HOME/bin:$PATH


# Save the original passwords to a file for use later to reset original passwords
ssh ${REMOTE_SYSTEM} "echo \"select 'update pg_authid set rolpassword = ''' || rolpassword || ''' where rolname = ''' || rolname || ''';' from pg_authid where rolpassword is not null and rolname not in ('postgres', 'enterprisedb', 'zenoss') order by rolname;\" | ${PG_HOME}/bin/${PSQL_EXEC} ${BASE_DB} -t > ~/change_passwords_to_original.${REMOTE_DBNAME}.sql"

# Backup conf files when they change. diff files, save new copy if file has changed, backup new file
ssh ${REMOTE_SYSTEM} "if (diff ~/postgresql.conf.${REMOTE_DBNAME} ${REMOTE_FILE_SYS_ROOT}/data/postgresql.conf > /dev/null); then :; else cp -v ${REMOTE_FILE_SYS_ROOT}/data/postgresql.conf ~/postgresql.conf.${REMOTE_DBNAME}; cp -v ~/postgresql.conf.${REMOTE_DBNAME} ~/refresh_backup_conf/postgresql.conf.${REMOTE_DBNAME}.`date '+%Y-%m-%d'`; fi;"

ssh ${REMOTE_SYSTEM} "if (diff ~/pg_hba.conf.${REMOTE_DBNAME} ${REMOTE_FILE_SYS_ROOT}/data/pg_hba.conf > /dev/null); then :; else cp -v ${REMOTE_FILE_SYS_ROOT}/data/pg_hba.conf ~/pg_hba.conf.${REMOTE_DBNAME}; cp -v ~/pg_hba.conf.${REMOTE_DBNAME} ~/refresh_backup_conf/pg_hba.conf.${REMOTE_DBNAME}.`date '+%Y-%m-%d'`; fi;"


# for debugging, list archive file before start of backup
ls -ltr ${LOCAL_FILE_SYS_ROOT}/arch

# for debugging
ssh ${REMOTE_SYSTEM} "ps axu | grep post"

ssh ${REMOTE_SYSTEM} "${PG_HOME}/bin/pg_ctl stop -D ${REMOTE_FILE_SYS_ROOT}/data -m immediate"

# for debugging
ssh ${REMOTE_SYSTEM} "ps axu | grep post"

ssh ${REMOTE_SYSTEM} "cd ${REMOTE_FILE_SYS_ROOT}; rm -rfv ${REMOTE_FILE_SYS_ROOT}/data/*"
ssh ${REMOTE_SYSTEM} "cd ${REMOTE_FILE_SYS_ROOT}; rm -rfv ${REMOTE_FILE_SYS_ROOT}/arch/*"

#################################

# put db in hotbackup mode
${PSQL_EXEC} ${LOCAL_DBNAME} --tuples-only --command "checkpoint;"
${PSQL_EXEC} ${LOCAL_DBNAME} --tuples-only --command "select pg_switch_xlog();"
${PSQL_EXEC} ${LOCAL_DBNAME} --tuples-only --command "SELECT pg_start_backup('refresh_${REMOTE_DBNAME}');"


cd ${LOCAL_FILE_SYS_ROOT}
tar cvf - ./data --exclude './data/${LOG_DIR}/*' --exclude './data/pg_xlog/0000*' --exclude './data/pg_xlog/archive_status/0000*' | ssh ${REMOTE_SYSTEM} "cd ${REMOTE_FILE_SYS_ROOT}; tar xf -"


${PSQL_EXEC} ${LOCAL_DBNAME} --tuples-only --command "SELECT pg_stop_backup();"
${PSQL_EXEC} ${LOCAL_DBNAME} --tuples-only --command "select pg_switch_xlog();"

#NOTE: it would be better to test the file sizes rather than use sleep
# Wait till archive log from log switch has finished writing before running tar
# otherwise we get a short archive log and the recovery on remote host fails
echo "sleeping for 120 seconds to allow last archive log to be written out"
sleep 120



# for debugging, list archive file before start of tar copy to remote server
ls -ltr ${LOCAL_FILE_SYS_ROOT}/arch

tar cvf - ./arch | ssh ${REMOTE_SYSTEM} "cd ${REMOTE_FILE_SYS_ROOT}; tar xf -"


#################################


ssh ${REMOTE_SYSTEM} "rm -v ${REMOTE_FILE_SYS_ROOT}/data/postgresql.conf*"
ssh ${REMOTE_SYSTEM} "rm -v ${REMOTE_FILE_SYS_ROOT}/data/pg_hba.conf*"
ssh ${REMOTE_SYSTEM} "rm -v ${REMOTE_FILE_SYS_ROOT}/data/postmaster.pid"
ssh ${REMOTE_SYSTEM} "rm -v ${REMOTE_FILE_SYS_ROOT}/data/backup_label"
ssh ${REMOTE_SYSTEM} "rm -v ${REMOTE_FILE_SYS_ROOT}/data/pg_log/*.log*"

ssh ${REMOTE_SYSTEM} "cp -v ~/postgresql.conf.${REMOTE_DBNAME} ${REMOTE_FILE_SYS_ROOT}/data/postgresql.conf"
ssh ${REMOTE_SYSTEM} "cp -v ~/pg_hba.conf.${REMOTE_DBNAME} ${REMOTE_FILE_SYS_ROOT}/data/pg_hba.conf"
ssh ${REMOTE_SYSTEM} "cp -v ~/recovery.conf.${REMOTE_DBNAME} ${REMOTE_FILE_SYS_ROOT}/data/recovery.conf"

ssh ${REMOTE_SYSTEM} "${PG_HOME}/bin/pg_ctl -w start -D ${REMOTE_FILE_SYS_ROOT}/data -l ${REMOTE_FILE_SYS_ROOT}/data/${LOG_DIR}/startup.log"

# change name of database and owner of schemas
ssh ${REMOTE_SYSTEM} "${PG_HOME}/bin/${PSQL_EXEC} ${BASE_DB} --tuples-only --command 'ALTER DATABASE ${LOCAL_DBNAME} RENAME TO ${REMOTE_DBNAME};'"
ssh ${REMOTE_SYSTEM} "${PG_HOME}/bin/${PSQL_EXEC} ${BASE_DB} --tuples-only --command 'ALTER USER ${LOCAL_DBNAME} RENAME TO ${REMOTE_DBNAME};'"


# Create script to change all user passwords to a default
ssh ${REMOTE_SYSTEM} "echo \"select 'alter user ' || rolname || ' with password ''beer123'';' from pg_authid where rolpassword is not null and rolname not in ('postgres', 'enterprisedb', 'zenoss') order by rolname;\" | ${PG_HOME}/bin/${PSQL_EXEC} ${BASE_DB} -t > ~/change_passwords_to_defaults.${REMOTE_DBNAME}.sql"

# change passwords to default
ssh ${REMOTE_SYSTEM} "${PG_HOME}/bin/${PSQL_EXEC} ${REMOTE_DBNAME} < ~/change_passwords_to_defaults.${REMOTE_DBNAME}.sql"

# change passwords back to original
ssh ${REMOTE_SYSTEM} "${PG_HOME}/bin/${PSQL_EXEC} ${REMOTE_DBNAME} < ~/change_passwords_to_original.${REMOTE_DBNAME}.sql"

# grab server log from remote db
ssh ${REMOTE_SYSTEM} "cat ${REMOTE_FILE_SYS_ROOT}/data/${LOG_DIR}/postgresql-`date +%Y-%m-%d`_*.log" > /home/logs/${REMOTE_DBNAME}.dbserver.log

echo "END: `date`"

cat /home/logs/refresh.${REMOTE_DBNAME}_from_${LOCAL_DBNAME}.mail /home/logs/${REMOTE_DBNAME}.dbserver.log >>  /home/logs/refresh.${REMOTE_DBNAME}_from_${LOCAL_DBNAME}.log

cat /home/logs/refresh.${REMOTE_DBNAME}_from_${LOCAL_DBNAME}.mail /home/logs/${REMOTE_DBNAME}.dbserver.log | mail -s refresh.${REMOTE_DBNAME}_from_${LOCAL_DBNAME}.mail pgeever@company.com
