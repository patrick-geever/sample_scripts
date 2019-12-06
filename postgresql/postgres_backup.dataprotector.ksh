#! /bin/ksh
#

NAME="postgres_backup.dataprotector.ksh"
BASE=`dirname $0`
[[ "$BASE" = "." ]] && BASE=`pwd`
HOST=`hostname`
TAG=`date '+%Y%m%d%H%M%S'`
CURRENT_USER=`whoami`
FILEPURGEDAYS="7"                               # Maximum age of trace files to purge

################################################################
# Functions
################################################################

showsyntax_and_exit()
{
  echo "-------------------------------------------------------------------------------------"
  echo "Usage :         ${NAME} -a <Action> [Options]"
  echo
  echo "Function :      Places a Postgres cluster in/out of backup mode or purges archives"
  echo "                prior to most recent backup."
  echo
  echo "  -a <Action>           Specify the action to perform.  Defined actions are :"
  echo "                        start, stop, purgearc"
  echo
  echo "Optional Flags : "
  echo
  echo "  -d <DB>               Specify the Postgres DB"
  echo "  -u <User>             Specify the Postgres user."
  echo "                        *defaults to postgres or edb (Depending on binaries detected)"
  echo "  -h <Hostname>         Specify the Postgres hostname"
  echo "  -p <Port>             Specify the Postgres listener port"
  echo "  -s <BackupSpec>       Specify the DataProtector Backup Specification"
  echo "-------------------------------------------------------------------------------------"
  echo
exit 8
}

#---------------------------------------------------------------
# Function ExecSQL
#---------------------------------------------------------------
ExecSQL()
{
  StrSQL=$1
  ExecSQL=`$PSQL_CMD -d $DB -U $DBUSER -h $DBHOST -p $DBPORT --tuples-only -c "$StrSQL" 2>&1`
  echo "$ExecSQL"
}



################################################################
# Main Program
################################################################
if [[ $# -le 1 ]] ; then
  echo "Error: To few arguments"
  showsyntax_and_exit
fi

while getopts a:d:u:h:p:s: opt; do
  case $opt in
    a) ACTION=`echo ${OPTARG} | tr "[:lower:]" "[:upper:]"`;;
    d) DB="${OPTARG}";;
    u) DBUSER="${OPTARG}";;
    h) DBHOST="${OPTARG}";;
    p) DBPORT="${OPTARG}";;
    s) BACKUPSPEC="${OPTARG}";;
    \?) echo "Error: Bad option ${OPTARG} or missing argument."
        showsyntax_and_exit;;
  esac
done

#---------------------------------------------------------------
# Ensure a valid action.
#---------------------------------------------------------------
case "$ACTION" in
  "START") ;;
  "STOP")  ;;
  "PURGEARC")  ;;
   *)     echo "Error: Bad -a argument."
           showsyntax_and_exit;;
esac

#---------------------------------------------------------------
# Determine native Postgres or EnterpriseDB.
#---------------------------------------------------------------
if [[ -d $PGDATA/edb_network ]]; then
  DBTYPE="enterprisedb"
  # Assign DBUSER/DBPORT if NULL
  [[ -z $DB ]] && DB="edb"
  [[ -z $DBUSER ]] && DBUSER="enterprisedb"
  [[ -z $DBHOST ]] && DBHOST=`hostname`
  [[ -z $DBPORT ]] && DBPORT="5444"
  PSQL_CMD="edb-psql"
else
  DBTYPE="postgres"
  # Assign DBUSER/DBPORT if NULL
  [[ -z $DB ]] && DB="postgres"
  [[ -z $DBUSER ]] && DBUSER="postgres"
  [[ -z $DBHOST ]] && DBHOST=`hostname`
  [[ -z $DBPORT ]] && DBPORT="5432"
  PSQL_CMD="psql"
fi

#---------------------------------------------------------------
# Get directory locations.
#---------------------------------------------------------------
DATA_DIR=`ExecSQL "show data_directory;" | tr -d ' '`
ARCH_DIR=`ExecSQL "show archive_command;" | awk '{print $4}' | awk -F/ '{for (i=2;i<NF;i++){printf "/"$i;}printf "\n";}'`
LOG_DIR=`ExecSQL "show log_directory;"`

LOCAL_LOGFILE=/home/logs/postgres_backup.dataprotector.${DBHOST}.log

#---------------------------------------------------------------
# Show Params.
#---------------------------------------------------------------
date
echo "---------------------------------------------------------------"
echo "${NAME} executing with the following parameters as user ${CURRENT_USER}"
echo "ACTION = $ACTION"
echo "DBTYPE = $DBTYPE"
echo "DB = $DB"
echo "DBUSER = $DBUSER"
echo "DBHOST = $DBHOST"
echo "DBPORT = $DBPORT"
echo "DATA_DIR = $DATA_DIR"
echo "ARCH_DIR = $ARCH_DIR"
echo "LOG_DIR = $LOG_DIR"
echo "LOCAL_LOGFILE = $LOCAL_LOGFILE"
echo "BACKUPSPEC = $BACKUPSPEC"
echo "---------------------------------------------------------------"

#---------------------------------------------------------------
# Ensure we have no errors connecting to the DB.
#---------------------------------------------------------------
echo "$DATA_DIR $LOG_DIR" | grep FATAL > /dev/null
if [[ $? = 0 ]];  then
  echo "Fatal error detected!!! EXITING."
  exit 8
fi
#exit 99

#---------------------------------------------------------------
# Action = start
#---------------------------------------------------------------
if [[ $ACTION = "START" ]];  then

  #pg write to local logfile
  echo "${ACTION}: `date`" >> ${LOCAL_LOGFILE}

  # Check to see if db is already in a backup mode. If so, run pg_stop_backup().
  if [[ -f ${DATA_DIR}/backup_label ]]; then
    echo "ERROR: Database is already in backup mode!!!"
    exit 8
  fi

  # Add checkpoint command here to speed up the start of the backup (see p. 458 in manual)
  echo "Forcing Checkpoint"
  SQLRC=`ExecSQL "checkpoint;"`
  echo "$SQLRC"
  echo "$SQLRC" | grep -e FATAL -e ERROR > /dev/null
  if [[ $? = 0 ]];  then
    echo "Fatal error detected.  EXITING."
    exit 8
  fi

  # Force a log switch
  echo "Forcing log switch"
  SQLRC=`ExecSQL "select pg_switch_xlog();"`
  echo "$SQLRC"
  echo "$SQLRC" | grep -e FATAL -e ERROR > /dev/null
  if [[ $? = 0 ]];  then
    echo "Fatal error detected.  EXITING."
    exit 8
  fi

  # Place database in hot backup mode
  echo "Attempting to place database in backup mode"
  SQLRC=`ExecSQL "SELECT pg_start_backup('${HOST}_${TAG}_Backup');"`
  echo "$SQLRC"
  echo "$SQLRC" | grep -e FATAL -e ERROR > /dev/null
  if [[ $? = 0 ]];  then
    echo "Fatal error detected.  EXITING."
    exit 8
  fi
fi

#---------------------------------------------------------------
# Action = stop
#---------------------------------------------------------------
if [[ $ACTION = "STOP" ]];  then

  #pg write to local logfile
  echo "${ACTION}: `date`" >> ${LOCAL_LOGFILE}

  if [[ ! -f ${DATA_DIR}/backup_label ]]; then
    echo "ERROR: Database is not in backup mode!!!"
    exit 8
  fi

  # Remove the database from hot backup mode
  echo "Attempting to take database out of backup mode"
  SQLRC=`ExecSQL "SELECT pg_stop_backup();"`
  echo "$SQLRC"
  echo "$SQLRC" | grep -e FATAL -e ERROR > /dev/null
  if [[ $? = 0 ]];  then
    echo "Fatal error detected.  EXITING."
    exit 8
  fi

  # Force a log switch
  echo "Forcing log switch"
  SQLRC=`ExecSQL "select pg_switch_xlog();"`
  echo "$SQLRC"
  echo "$SQLRC" | grep -e FATAL -e ERROR > /dev/null
  if [[ $? = 0 ]];  then
    echo "Fatal error detected.  EXITING."
    exit 8
  fi

# Kick off WAL/archive_log backup immediately using omnib cli command
# NOTE: postgres/enterprisedb user must be added to DataProtector as admin
  if [ ${BACKUPSPEC} ]
  then
    echo "Starting WAL/archive_log backup ${BACKUPSPEC}"
    echo "Starting WAL/archive_log backup ${BACKUPSPEC}" >> ${LOCAL_LOGFILE}
    SQLRC=`/opt/omni/bin/omnib -datalist ${BACKUPSPEC}`
    echo "$SQLRC"
    echo "$SQLRC" | grep -e FATAL -e ERROR > /dev/null
    if [[ $? = 0 ]];  then
      echo "Fatal error detected.  EXITING."
      exit 8
    fi
  fi
fi

#---------------------------------------------------------------
# Action =purgearc
#---------------------------------------------------------------
if [[ $ACTION = "PURGEARC" ]];  then

  #pg write to local logfile
  echo "${ACTION}: `date`" >> ${LOCAL_LOGFILE}

  FIRST_LOG_NEEDED=`ls -ltr ${ARCH_DIR}/*.backup | tail -1 | awk '{print $9}' | awk -F/ '{print $NF}' | awk -F. '{print $1}'`
  echo "Purging archiveLogs from ${ARCH_DIR} older then : $FIRST_LOG_NEEDED"
  echo "Purging archiveLogs from ${ARCH_DIR} older then : $FIRST_LOG_NEEDED" >> ${LOCAL_LOGFILE}
  OLDEST_LOG_TO_PURGE=`ls -ltr ${ARCH_DIR} | grep -v backup | awk '{print $9}' | awk -F/ '{print $NF}' | awk '/'"${FIRST_LOG_NEEDED}"'/ {print x }; {x=$0}'`
  find ${ARCH_DIR} -type f ! -newer ${ARCH_DIR}/${OLDEST_LOG_TO_PURGE} -exec rm {} \;
fi

#---------------------------------------------------------------
# Purge LOG Files older then 7 days
#---------------------------------------------------------------
echo "Purging files in ${LOG_DIR} older then ${FILEPURGEDAYS} days"
(
  find ${LOG_DIR} -mtime +${FILEPURGEDAYS} -exec rm {} \;
) > /dev/null 2>&1

echo "Done RC:0"
exit 0
