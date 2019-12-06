#!/usr/bin/perl

# Source common ENV variables
do "/servers/config/gdr.env.pl";

use File::Basename;
$scriptname = basename ($0);
system("$tracker_script $scriptname $tracker_number $tracker_hostname START");

$source_table = "COMPANY.GT_EURO_HOURLY";
$target_table = "EURO_ANALYSIS.GT_EURO_HOURLY";


# Email addresses or commands must be wrapped in if/else to make sure that emails to 
# production email addresses are only sent when script runs on the productions system 
if($mailflag eq 'GDRPRD') {
   @to_address = "GDROperations\@company.com pgeever\@company.com";
} else {
   @to_address = "pgeever\@company.com";
}


$sync_job_name = "$source_table" . "_to_" . "$target_table";

print "START: " . `date`;
print "JOB NAME: $sync_job_name\n";


use DBI;
use DBD::Pg;
use DBD::Oracle;

sub send_error_notification {
    my ($error_message_string) = @_;
    print "JOB NAME: $sync_job_name\n";
    print "Sending message that an error has occurred: " . $error_message_string . "\n";
    `echo "$error_message_string" | mail -s "ERROR: $sync_job_name($mailflag) Sync Job" @to_address`;
}

# Set Error printing on
my %attr = ( PrintError => 1, RaiseError => 0); 


# EUROPEAN POWER
my $dbh_ora=DBI->connect("dbi:Oracle:host=europroddb.company.com;port=1521;sid=europrod", "genview", "icugen",\%attr);
if (!defined $dbh_ora) {
    $error_message = "Can't connect to Oracle database: " .  $DBI::errstr;
    send_error_notification($error_message);
    die "$error_message\n";
}


my $dbh_pg=DBI->connect("DBI:Pg:dbname=$dbname;host=$dbhostname;port=$dbport", "$dbuser", "$dbpassword", \%attr);
if (!defined $dbh_pg) {
    $error_message = "Can't connect to Postgres database: " .  $DBI::errstr;
    send_error_notification($error_message);
    die "$error_message\n";
}

#####

# START: gt_euro_hourly table refresh

# Turn off error flag
$insert_error_flag = 0;

# get max gmtdate from target table in EDB and store in $max_gmtdate
# We need to grab all rows greater than that date
my $sql = qq{ select to_char( max(gmtdate), 'YYYY-MM-DD HH24:MI:SS') from euro_analysis.gt_euro_hourly };
my $sth_pg = $dbh_pg->prepare($sql);
if(!defined $sth_pg) {
    $error_message = "failed to prepare";
    send_error_notification($error_message);
    die "$error_message\n";
}

my $test = $sth_pg->execute();
if(!defined $test) {
    $error_message = "failed to execute: " . $sth_pg->errstr;
    send_error_notification($error_message);
    die "$error_message\n";
}
#print "test = $test\n";

my $test2 = $sth_pg->bind_columns(\$dbiout);
if(!defined $test2) {
    $error_message = "failed to bind columns: " . $sth_pg->errstr;
    send_error_notification($error_message);
    die "$error_message\n";
}
#print "test2 = $test2\n";

while( $sth_pg->fetch() ) {
    $max_gmtdate = $dbiout;
    print "Target Table: $target_table: max_gmtdate = $max_gmtdate - Before sync\n";
}


## first get count of number of rows where gmtdate is greater than $max_gmtdate
my $sql = qq{ select count(*) from company.gt_euro_hourly where  gmtdate > to_date('$max_gmtdate', 'YYYY-MM-DD HH24:MI:SS') };

#print "$sql\n";

my $sth_ora = $dbh_ora->prepare( $sql );
if(!defined $sth_ora) {
    $error_message = "failed to prepare";
    send_error_notification($error_message);
    die "$error_message\n";
}

my $test = $sth_ora->execute();
if(!defined $test) {
    $error_message = "failed to execute: " . $sth_ora->errstr;
    send_error_notification($error_message);
    die "$error_message\n";
}
#print "test = $test\n";


my $test2 = $sth_ora->bind_columns(\$rows);
if(!defined $test2) {
    $error_message = "failed to bind columns: " . $sth_ora->errstr;
    send_error_notification($error_message);
    die "$error_message\n";
}
#print "test2 = $test2\n";


$sth_ora->fetch(); 
print "Source Table: $source_table: rows = $rows\n";


## then select all row data where gmtdate is greater than $max_gmtdate
my $sql = qq{ 
    select entity_id, to_char(gmtdate, 'YYYY-MM-DD HH24:MI:SS'), 
    gmthour, to_char(eurodate, 'YYYY-MM-DD HH24:MI:SS'),
    company_mw, to_char(time_stamp, 'YYYY-MM-DD HH24:MI:SS')
	from  company.gt_euro_hourly
	where  gmtdate > to_date('$max_gmtdate', 'YYYY-MM-DD HH24:MI:SS')
    };

#print "$sql\n";

my $sth_ora = $dbh_ora->prepare( $sql );
if(!defined $sth_ora) {
    $error_message = "failed to prepare";
    send_error_notification($error_message);
    die "$error_message\n";
}


my $test = $sth_ora->execute();
if(!defined $test) {
    $error_message = "failed to execute: " . $sth_ora->errstr;
    send_error_notification($error_message);
    die "$error_message\n";
}
#print "test = $test\n";


my ($entity_id, $gmtdate, $gmthour, $eurodate, $company_mw, $time_stamp);

my $test2 = $sth_ora->bind_columns(\$entity_id, \$gmtdate, \$gmthour, \$eurodate, \$company_mw, \$time_stamp);
if(!defined $test2) {
    $error_message = "failed to bind columns: " . $sth_ora->errstr;
    send_error_notification($error_message);
    die "$error_message\n";
}
#print "test2 = $test2\n";


my $insert_sql = qq{ INSERT INTO euro_analysis.gt_euro_hourly 
			 ( entity_id, gmtdate, gmthour, eurodate, company_mw, time_stamp
			   )
			 VALUES 
			 ( ?, to_date(?, 'YYYY-MM-DD HH24:MI:SS'),
			   ?, to_date(?, 'YYYY-MM-DD HH24:MI:SS'),
			   ?, to_date(?, 'YYYY-MM-DD HH24:MI:SS') ) };

$sth_pg = $dbh_pg->prepare($insert_sql);
if(!defined $sth_pg) {
    $error_message = "failed to prepare";
    send_error_notification($error_message);
    die "$error_message\n";
}

while( $sth_ora->fetch ) {
    $sth_pg->bind_param(1, $entity_id);
    $sth_pg->bind_param(2, $gmtdate);
    $sth_pg->bind_param(3, $gmthour);
    $sth_pg->bind_param(4, $eurodate);
    $sth_pg->bind_param(5, $company_mw);
    $sth_pg->bind_param(6, $time_stamp);
    
    #print "$entity_id, $gmtdate, $gmthour, $eurodate, $company_mw, $time_stamp\n";
    
    # Insert!
    my $insert_test = $sth_pg->execute();
    
    if ($insert_test eq 1 ) {
	$insert_count = $insert_count + 1; 
    } else {
	$insert_error_flag = 1;
	$insert_error_message = "insert_failed: " . $sth_pg->errstr;
    }
    
    $insert_test = 0;
}

print "Target Table: $target_table: Inserts = $insert_count\n";


############################ Check max(gmtdate) after sync completes:
my $sql = qq{ select to_char( max(gmtdate), 'YYYY-MM-DD HH24:MI:SS') from euro_analysis.gt_euro_hourly };
my $sth_pg = $dbh_pg->prepare($sql);
if(!defined $sth_pg) {
    $error_message = "failed to prepare";
    send_error_notification($error_message);
    die "$error_message\n";
}

my $test = $sth_pg->execute();
if(!defined $test) {
    $error_message = "failed to execute: " . $sth_pg->errstr;
    send_error_notification($error_message);
    die "$error_message\n";
}
#print "test = $test\n";

my $test2 = $sth_pg->bind_columns(\$dbiout);
if(!defined $test2) {
    $error_message = "failed to bind columns: " . $sth_pg->errstr;
    send_error_notification($error_message);
    die "$error_message\n";
}
#print "test2 = $test2\n";

while( $sth_pg->fetch() ) {
    $max_gmtdate = $dbiout;
    print "Target Table: $target_table: max_gmtdate = $max_gmtdate - After sync\n";
}


#########################


# Send notification that there were insert errors
if ( ($rows gt 0  && $rows ne $insert_count) || $insert_error_flag ne 0) {
    $error_message = "Expected Rows = $rows : Inserted_Rows = $insert_count: $insert_error_message";
    send_error_notification($error_message);
}

# END: gt_euro_hourly table refresh

print "END: " . `date` . "\n";
system("$tracker_script $scriptname $tracker_number $tracker_hostname END");


