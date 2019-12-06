#!/bin/bash

# Source common variables
. /servers/config/gdr.env.sh

# Get script name and run tracker script
SCRIPT_NAME=`basename $0`; export SCRIPT_NAME
eval $TRACKER_SCRIPT $SCRIPT_NAME $TRACKER_NUMBER $TRACKER_HOSTNAME START

LOGFILE=/home/logs/P_INSERT_COALBURN_ADJ_WEEKLY.proc.log
MAILFILE=/home/logs/P_INSERT_COALBURN_ADJ_WEEKLY.proc.mail

echo "START: `date`" >> $MAILFILE

edb-psql -h ${DBHOSTNAME} -p ${DBPORT} -d ${DBNAME} -U ${DBUSER} <<EOF >> $MAILFILE
call COALBURN_RPT.P_INSERT_COALBURN_ADJ_WEEKLY();
EOF

echo "END: `date`" >> $MAILFILE

cat $MAILFILE >> $LOGFILE

if [ ${MAILFLAG} = "GDRPRD" ]
then
  cat $MAILFILE | mail -s "COALBURN_RPT.P_INSERT_COALBURN_ADJ_WEEKLY run in ${MAILFLAG}" GDROperations@company.com
else
  cat $MAILFILE | mail -s "COALBURN_RPT.P_INSERT_COALBURN_ADJ_WEEKLY run in ${MAILFLAG}" $DEVMAILLIST
fi

# Run tracker script for end
eval $TRACKER_SCRIPT $SCRIPT_NAME $TRACKER_NUMBER $TRACKER_HOSTNAME END


