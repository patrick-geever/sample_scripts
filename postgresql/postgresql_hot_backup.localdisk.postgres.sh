#!/bin/bash

# am i running as postgres check
WHOAMI_USER=`whoami`
if [ ${WHOAMI_USER} != "postgres" -a ${WHOAMI_USER} != "enterprisedb" ]
then
        echo "Error: This script MUST be run as postgres or enterprisedb user"
	echo "WHOAMI_USER = $WHOAMI_USER"
        exit
fi


echo "START: `date`"

# set -e
umask 077

HOST=`hostname`
DATE=$(date "+%Y%m%d")

if [ ${WHOAMI_USER} = "postgres" ]
then
	PORT=5432; export PORT
	DATABASE=postgres; export DATABASE
	PGBIN_PATH=/opt/PostgreSQL/9.1/bin; export PGBIN_PATH
elif [ ${WHOAMI_USER} = "enterprisedb" ]
then
	PORT=5444; export PORT
	DATABASE=edb; export DATABASE
	PGBIN_PATH=/opt/PostgresPlus/9.1AS/bin; export PGBIN_PATH
fi

CLUSTER_DIR=`dirname $PGDATA`; export CLUSTER_DIR
echo "CLUSTER_DIR = $CLUSTER_DIR"

PATH=$PATH:$HOME/bin; export PATH
PGHOST=localhost; export PGHOST
PGDATA_PATH=`echo "$PGDATA" | tr '/' '-' | sed -e 's/^-//'`; export PGDATA_PATH
PGBACKUP_DIR=${CLUSTER_DIR}/backup; export PGBACKUP_DIR
PGDATA_DIR=`basename "$PGDATA"`; export PGDATA_DIR
PGARCH_DIR=${CLUSTER_DIR}/arch; export PGARCH_DIR
PSQL_BINARY=psql; export PSQL_BINARY


# NOTE: check to see if db is already in a backup mode. If so, run pg_stop_backup().
if [ -f ${PGDATA}/backup_label ]
then
	echo "Backup in progress: running pg_stop_backup\(\)"
	stop_old_backup_out=`${PGBIN_PATH}/${PSQL_BINARY} -d ${DATABASE} -p "$PORT" --tuples-only --command "SELECT pg_stop_backup();"`
	echo "stop_old_backup_out = $stop_old_backup_out"
fi


# find start WAL log number before backup starts
CURRENT_LOGFILE=`${PGBIN_PATH}/${PSQL_BINARY} -d ${DATABASE} -p "$PORT" --tuples-only --command "select pg_xlogfile_name(pg_current_xlog_location());" | tr -d ' '`
echo "CURRENT_LOGFILE = $CURRENT_LOGFILE"


# NOTE: add checkpoint command here to speed up the start of the backup (see p. 458 in manual)
checkpoint_out=`${PGBIN_PATH}/${PSQL_BINARY} -d ${DATABASE} -p "$PORT" --tuples-only --command "checkpoint;"`
echo "checkpoint_out = $checkpoint_out"

#NOTE: force a log switch
# pg_switch_xlog()
log_switch1=`${PGBIN_PATH}/${PSQL_BINARY} -d ${DATABASE} -p "$PORT" --tuples-only --command "select pg_switch_xlog();"`
echo "log_switch1 = $log_switch1"

# sleep for 90 seconds to age last logfile at least a minute for the command file later
SLEEP_TIME=90
echo "sleeping $SLEEP_TIME"
sleep $SLEEP_TIME

LABEL="$DATE-$HOST-$PGDATA_PATH-backup"
ID=`${PGBIN_PATH}/${PSQL_BINARY} -d ${DATABASE} -p "$PORT" --tuples-only --command "SELECT pg_start_backup('$LABEL');"`
ID=`echo $ID | sed -e 's/[^a-zA-Z0-9]/_/g'`
echo "ID = $ID"


cd /alpha
# NOTE: tar must ignore warnings that a file changed during copying. This is okay. It should only fail on real errors.
# (see p. 459 in manual) Should be a return code of 1 from tar versions 1.16 or greater.
TARBALL="${PGBACKUP_DIR}/${LABEL}-${ID}.tar"
TARBALL_ARCH="${PGBACKUP_DIR}/${LABEL}-${ID}.archive.tar"
cd ${PGDATA}/..
if tar cf "$TARBALL" --exclude "${PGDATA_DIR}/pg_xlog" "${PGDATA_DIR}"; then
	:  # all went well
else
	if [ "$?" = 1 ]; then
		echo "Ignoring tar exit code of 1" >&2
	else
		echo "Dying because tar exited with an exit code not in {0, 1}" >&2
		exit 1
	fi
fi


stop_backup_out=`${PGBIN_PATH}/${PSQL_BINARY} -d ${DATABASE} -p "$PORT" --tuples-only --command "SELECT pg_stop_backup();"`
echo "stop_backup_out = $stop_backup_out"

sleep 5

# NOTE: Perhaps add pg_xlog and archive_status directories as empty directories to the tar file.
cd /alpha

#NOTE: force a log switch
# pg_switch_xlog()
log_switch2=`${PGBIN_PATH}/${PSQL_BINARY} -d ${DATABASE} -p "$PORT" --tuples-only --command "select pg_switch_xlog();"`

# wait for log switch to complete
wait
echo "log_switch2 = $log_switch2"


# NOTE: use find command to get all wal logs since beginning of backup
cd ${PGARCH_DIR}
#find . -type f  -newer ${CURRENT_LOGFILE} -exec nice tar -rvf "$TARBALL" {} \;
find . -type f  -newer ${CURRENT_LOGFILE} -exec nice tar -cvf "$TARBALL_ARCH" {} \;

# compress backup file
gzip --fast "$TARBALL"
gzip --fast "$TARBALL_ARCH"

echo "END: `date`"

