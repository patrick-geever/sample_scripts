#!/usr/bin/perl

# GDR ENV variables to be sourced in perl scripts
#
$ENV{'EDB_HOME'} = '/opt/PostgresPlus/8.4AS';
$ENV{'EDBHOME'} = $ENV{'EDB_HOME'};
$ENV{'PATH'} = "$ENV{'EDB_HOME'}/bin:/usr/java/default/bin:/usr/local/bin:/bin:/usr/bin:/usr/oracle/instantclient";


# DB variables. These allow scripts to connect to the correct database and userid
$dbhostname = 'gdrqadb.company.com';
$dbport = '5444';
$dbname = 'gdrqa';
$dbuser = 'gdrqa';
$dbpassword = 'gdrqa';

#Set DB ENV shell variables in perl script. These allow O/S shell scripts to connect to the correct database and userid
$ENV{'DBHOSTNAME'} = $dbhostname;
$ENV{'DBPORT'} = $dbport;
$ENV{'DBNAME'} = $dbname;
$ENV{'DBUSER'} = $dbuser;
$ENV{'DBPASSWORD'} = $dbpassword;


# Mail Flag to indicate if and/or where emails should be sent
# Acceptable values: GDRPRD or GDRQA or GDRTEST or GDRDEV
$mailflag = 'GDRQA';
$devmaillist = "pgeever\@company.com";


# Tracker Script variables
use Sys::Hostname;
$tracker_script = '/servers/config/tracker_script.sh';
$tracker_number = time();
$tracker_hostname = hostname(); 
