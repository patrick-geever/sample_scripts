#!/usr/bin/perl -l

use Env;

if ($#ARGV == 0)
{

    # If sid has domain included, remove domain name.
    @SidArray = split(/\./, $ARGV[0]);

    # Initially set $Sid = lowercase for Linux Environment
    $Sid = `echo $SidArray[0] | /usr/bin/tr [A-Z] [a-z]`;
    chomp($Sid);
} else
{
    print "\nUsage: CheckOraTablespaces.pl <Sid>\n";
    exit;
}

$ORACLE_HOME = '/oracle/prod/102_64';
$LD_LIBRARY_PATH = '/oracle/prod/102_64/lib32:/lib64:/lib:/usr/lib64:/usr/lib';
$ENV{'ORACLE_HOME'} = $ORACLE_HOME;
$ENV{'PATH'} = '/bin:/usr/bin:' . $ORACLE_HOME . '/bin:' . $PATH;
$ENV{'LD_LIBRARY_PATH'} = $LD_LIBRARY_PATH;
$ENV{'ORACLE_SID'} = $Sid;                                                                                  

# Now set $Sid = uppercase for logs and emails
$Sid = `echo $Sid | /usr/bin/tr [a-z] [A-Z]`;
chomp($Sid);

$PerCent = 90;
$junkline = '';
@OutOfSpaceTablespaces = '';
$OutOfSpaceFlag = 0;
$Rows = 0;
$Mail = '/home/logs/CheckOraTablespaces' . $Sid . '.mail';
$Log = '/home/logs/CheckOraTablespaces'  . $Sid . '.log';
$InsertErrorFlag = 0;
$DbLogin = "capacity";
$DbPassWord = "xxxxxx";
$DbTnsString = "jira.company.com";

chdir('/usr/local/company');
@junk = `rm $Mail`;

open(MAIL,"> $Mail");
open(LOG,">> $Log");

$SHour = `/bin/date '+%H'`;
chomp($SHour);

$SMin = `/bin/date '+%M'`;
chomp($SMin);

$Date = `/bin/date '+%D %H:%M %Z'`;
chomp($Date);
print(LOG "START: $Date");
print(MAIL "START: $Date");

sub gomail {
    # Send emails to the interested parties
    @out = `cat $Mail | mail -s "CheckOraTablespaces($Sid): $_[1]" pgeever\@company.com`;
}


# Run query to find percentage of tablespace used.
@junk = `sqlplus -s '/ as sysdba' <<EOF
set pagesize 100
set feedback off
set heading off
select 
da.tablespace_name || ',' || 
 round(tb.TableSum/1024/1024,1) || ',' ||
  round(da.DataMax/1024/1024,1) || ',' ||
    round((tb.TableSum/da.DataMax)*100,1) || ',' ||
     round(da.DataMax/1024/1024 - tb.TableSum/1024/1024,1) 
from (select a.tablespace_name,
sum(decode(autoextensible,'YES',maxbytes,bytes)) as DataMax
      from   dba_data_files a
      group by a.tablespace_name) da,
          (select b.tablespace_name, sum(b.bytes) as TableSum
       from   dba_segments b
       group by b.tablespace_name) tb
where da.tablespace_name = tb.tablespace_name
and da.tablespace_name not like 'UNDO%';
   exit;
EOF`;


$counter = 1;
foreach $junkline (@junk)
{
    # We need to skip the first line of output which is always just a '\n'.
    if($counter == 1) {
        $counter++;
	next;
    }
    $_ = $junkline;
    chomp;
    @TablespaceData = split(/,/);


# Insert tablespace capacity info into db
$tablespace = $TablespaceData[0];
$space_used_mb = $TablespaceData[1];
$total_space_mb = $TablespaceData[2];
$percent_used = $TablespaceData[3];
$space_free_mb = $TablespaceData[4];

#@insert_result = `sqlplus -s $DbLogin/$DbPassWord\@$DbTnsString <<EOF
#insert into capacity.capacity values (
#\'$Sid\',
#\'$tablespace\',
#$total_space_mb,
#$space_used_mb,
#$space_free_mb,
#$percent_used,
#trunc(sysdate),
#sysdate
#);
#commit;
#   exit;
#EOF`;
#
## Test for successful insert into db
#if( @error_string = grep(/ERROR|ORA-/, @insert_result)  ) {
#   print MAIL "Insert Error:\n @insert_result";
#   print MAIL "Attempted to Insert the following values:";
#   print MAIL "SID: \t\t\t$Sid\nTABLESPACE: \t$tablespace\nTOTAL_SPACE_MB: \t$total_space_mb\nSPACE_USED_MB: \t$space_used_mb\nSPACE_FREE_MB: \t$space_free_mb\nPERCENT_USED: \t$percent_used\n";
#   $InsertErrorFlag = 1;
#}

    # Send warning if tablespace is greater than or equal to $PerCent
    if($percent_used >= $PerCent) {
	$OutOfSpaceFlag = 1;
	push(@OutOfSpaceTablespaces, "Tablespace $TablespaceData[0] is at $TablespaceData[3] Percent USED!\n");
    }
}

print MAIL @OutOfSpaceTablespaces;


if ($OutOfSpaceFlag != 0) { 
    @junk2 = `sqlplus -s '/ as sysdba' <<EOF
set pagesize 1000
set feedback off
set heading off
select '######## TABLESPACE USED/FREE REPORT ###########################################' from dual;
set heading on
select INSTANCE_NAME from v\\\$instance;

column "TOTAL SPACE USED (MB)" heading 'TOTAL SPACE|USED (MB)'
column "TOTAL SPACE (MB)" heading 'TOTAL|SPACE (MB)'
column "PERCENT USED" heading 'PERCENT|USED'
column "FREE SPACE AVAILABLE (MB)" heading 'FREE SPACE|AVAILABLE (MB)'

select
da.tablespace_name as "TABLESPACE",
 round(tb.TableSum/1024/1024,1) as "TOTAL SPACE USED (MB)",
  round(da.DataMax/1024/1024,1) as "TOTAL SPACE (MB)",
    round((tb.TableSum/da.DataMax)*100,1) as "PERCENT USED",
     round(da.DataMax/1024/1024 - tb.TableSum/1024/1024,1) as "FREE SPACE AVAILABLE (MB)"
from (select a.tablespace_name,
sum(decode(autoextensible,'YES',maxbytes,bytes)) as DataMax
      from   dba_data_files a
      group by a.tablespace_name) da,
          (select b.tablespace_name, sum(b.bytes) as TableSum
       from   dba_segments b
       group by b.tablespace_name) tb
where da.tablespace_name = tb.tablespace_name
and da.tablespace_name not like 'UNDO%';

set heading off
select '################################################################################' from dual;
set heading on
set feedback on
   exit;
EOF`;


    print LOG @OutOfSpaceTablespaces;
    print MAIL @junk2;
    gomail($Mail,"WARNING - Tablespace(s) at $PerCent% Full"); 

}

#if($InsertErrorFlag != 0) {
#    print(LOG "ERROR inserting into CAPACITY Table");
#    print(MAIL @junk);
#    print(LOG @junk);
#    gomail($Mail,"ERROR inserting into CAPACITY Table");
#}

# Close files
$Date = `/bin/date '+%D %H:%M %Z'`;
chomp($Date);
print(LOG "END: $Date");
print(MAIL "END: $Date");
close(LOG);
close(MAIL);

