#!/bin/bash

# Add at start of script
#########
# Start tracker
TRACKER_SCRIPT=/net/netapp2/dataservices/ScriptTracker/tracker_script.tibco.v1.sh; export TRACKER_SCRIPT
TRACKER_NUMBER=`date +%s`; export TRACKER_NUMBER
TRACKER_HOSTNAME=`hostname`; export TRACKER_HOSTNAME
OSUSER=`whoami`
RETURN_CODE=0
ERROR_MESSAGE=OK

# Get script name and run tracker script
SCRIPT_NAME=`basename $0`; export SCRIPT_NAME
SCRIPT_PATH=`dirname $0`; export SCRIPT_PATH
eval $TRACKER_SCRIPT -n $SCRIPT_NAME -i $TRACKER_NUMBER -h $TRACKER_HOSTNAME -p $SCRIPT_PATH -u $OSUSER -s START > /dev/null
#########

echo
echo
echo "some code here"
random_sleep_time=`expr $RANDOM % 10`
echo "sleeping: $random_sleep_time"
sleep $random_sleep_time
echo
echo

# if there is an error set these
RETURN_CODE=$random_sleep_time
ERROR_MESSAGE="test script running on `hostname`: return_code = $random_sleep_time"


# Add at end of script
#########
# End tracker 
eval $TRACKER_SCRIPT -n $SCRIPT_NAME -i $TRACKER_NUMBER -h $TRACKER_HOSTNAME -p $SCRIPT_PATH -u $OSUSER -r $RETURN_CODE -m "'$ERROR_MESSAGE'" -s END > /dev/null
#########

