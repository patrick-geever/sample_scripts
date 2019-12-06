# GDR ENV variables to be sourced in shell scripts
#
EDB_HOME=/opt/PostgresPlus/8.4AS; export EDB_HOME
EDBHOME=${EDB_HOME}; export EDBHOME
PATH=${EDB_HOME}/bin:/usr/java/default/bin:/usr/local/bin:/bin:/usr/bin:/usr/oracle/instantclient; export PATH
#
#
#
# DB variables. These allow scripts to connect to the correct database and userid
DBHOSTNAME=gdrqadb.company.com; export DBHOSTNAME
DBPORT=5444; export DBPORT
DBNAME=gdrqa; export DBNAME
DBUSER=gdrqa; export DBUSER
DBPASSWORD=gdrqa; export DBPASSWORD
#
#
#
# Mail Flag to indicate if and/or where emails should be sent
# Acceptable values: GDRPRD or GDRQA or GDRTEST or GDRDEV
MAILFLAG=GDRQA; export MAILFLAG
DEVMAILLIST="pgeever@company.com"; export DEVMAILLIST
#
#
# Experimental: Location of common jdbc.properties file for jFit
# Can be used to copy current jdbc file into right locations rather then have symbolic links point at file
JFIT_JDBC_PROP_FILE=/servers/config/jdbc.properties; export JFIT_JDBC_PROP_FILE
JFIT_JDBC_POSTGRESQL_PROP_FILE=/servers/config/jdbc.properties.postgresql; export JFIT_JDBC_POSTGRESQL_PROP_FILE
#
#
#
#TRACKER_SCRIPT=echo; export TRACKER_SCRIPT
TRACKER_SCRIPT=/servers/config/tracker_script.sh; export TRACKER_SCRIPT
TRACKER_NUMBER=`date +%s`; export TRACKER_NUMBER
TRACKER_HOSTNAME=`hostname`; export TRACKER_HOSTNAME
#
