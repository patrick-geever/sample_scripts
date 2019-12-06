#!/bin/bash

SCRIPT_NAME=$1
TRACKER_NUMBER=$2
TRACKER_HOSTNAME=$3
STATE=$4

if [ ${STATE} = "START" ]
then
  psql -h $DBHOSTNAME -d $DBNAME -p $DBPORT -U $DBUSER -c \
    "insert into appl_metrics.script_history (script_name, tracking_id, script_host, status, start_datetime) \
     values ('$SCRIPT_NAME', $TRACKER_NUMBER, '$TRACKER_HOSTNAME', '$STATE', now);"
elif [ ${STATE} = "END" ]
then
  psql -h $DBHOSTNAME -d $DBNAME -p $DBPORT -U $DBUSER -c \
    "update appl_metrics.script_history set end_datetime = now, status = 'END' \
     where script_name = '$SCRIPT_NAME' and tracking_id = $TRACKER_NUMBER;" 
else
  echo "print an error"
fi

