#!/usr/bin/perl

# Source common ENV variables
do "/servers/config/gdr.env.pl";

use File::Basename;
$scriptname = basename ($0);
system("$tracker_script $scriptname $tracker_number $tracker_hostname START");

$source_table = "COMPANY.ENTITY";
$target_table = "EV_ANALYSIS.ENTITY";

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


# NORTH AMERICAN POWER
my $dbh_ora=DBI->connect("dbi:Oracle:host=proddb.company.com;port=1521;sid=prod", "genview", "icugen",\%attr);
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

# START: entity table refresh

# Turn off error flag
$insert_error_flag = 0;

# delete all rows in entity table
my $sql = qq{ delete from ev_analysis.entity };
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
$deleted_rows = $sth_pg->rows;
print "Target Table: $target_table: deleted_rows = $deleted_rows\n";


## first get count of number of rows in entity table
my $sql = qq{ select count(*) from company.entity };

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



## then select all row data from entity table

my $sql = qq{ 
    select entity_id, entity_type_cd, entity_display_name, entity_icon_type_id, static_display_notes, entity_full_name, entity_description, stp_operating_capacity_dir1, stp_operating_capacity_dir2, stp_flowgate_name, plant_nameplate, plant_negative_capacity, latitude, longitude, plant_city, plant_county, plant_state_id, plant_zip, plant_code, energy_source_id, nerc_region_id, time_stamp, online_date, stp_primary_direction, stp_origin_type, stp_origin, stp_terminus_type, stp_terminus, active_ind, control_area, heat_rate, country_code, region_id, angle
	from company.entity };

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


my ($entity_id, $entity_type_cd, $entity_display_name, $entity_icon_type_id, $static_display_notes, $entity_full_name, $entity_description, $stp_operating_capacity_dir1, $stp_operating_capacity_dir2, $stp_flowgate_name, $plant_nameplate, $plant_negative_capacity, $latitude, $longitude, $plant_city, $plant_county, $plant_state_id, $plant_zip, $plant_code, $energy_source_id, $nerc_region_id, $time_stamp, $online_date, $stp_primary_direction, $stp_origin_type, $stp_origin, $stp_terminus_type, $stp_terminus, $active_ind, $control_area, $heat_rate, $country_code, $region_id, $angle);

my $test2 = $sth_ora->bind_columns
    (\$entity_id, \$entity_type_cd, \$entity_display_name, \$entity_icon_type_id, \$static_display_notes, \$entity_full_name, \$entity_description, \$stp_operating_capacity_dir1, \$stp_operating_capacity_dir2, \$stp_flowgate_name, \$plant_nameplate, \$plant_negative_capacity, \$latitude, \$longitude, \$plant_city, \$plant_county, \$plant_state_id, \$plant_zip, \$plant_code, \$energy_source_id, \$nerc_region_id, \$time_stamp, \$online_date, \$stp_primary_direction, \$stp_origin_type, \$stp_origin, \$stp_terminus_type, \$stp_terminus, \$active_ind, \$control_area, \$heat_rate, \$country_code, \$region_id, \$angle);
if(!defined $test2) {
    $error_message = "failed to bind columns: " . $sth_ora->errstr;
    send_error_notification($error_message);
    die "$error_message\n";
}
#print "test2 = $test2\n";


my $insert_sql = qq{ INSERT INTO ev_analysis.entity 
			 ( entity_id, entity_type_cd, entity_display_name, entity_icon_type_id, static_display_notes, entity_full_name, entity_description, stp_operating_capacity_dir1, stp_operating_capacity_dir2, stp_flowgate_name, plant_nameplate, plant_negative_capacity, latitude, longitude, plant_city, plant_county, plant_state_id, plant_zip, plant_code, energy_source_id, nerc_region_id, time_stamp, online_date, stp_primary_direction, stp_origin_type, stp_origin, stp_terminus_type, stp_terminus, active_ind, control_area, heat_rate, country_code, region_id, angle)
			 VALUES 
			 ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, to_date(?, 'YYYY-MM-DD HH24:MI:SS'), to_date(?, 'YYYY-MM-DD HH24:MI:SS'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) };

$sth_pg = $dbh_pg->prepare($insert_sql);
if(!defined $sth_pg) {
    $error_message = "failed to prepare";
    send_error_notification($error_message);
    die "$error_message\n";
}

while( $sth_ora->fetch ) {
    $sth_pg->bind_param(1, $entity_id);
    $sth_pg->bind_param(2, $entity_type_cd);
    $sth_pg->bind_param(3, $entity_display_name);
    $sth_pg->bind_param(4, $entity_icon_type_id);
    $sth_pg->bind_param(5, $static_display_notes);
    $sth_pg->bind_param(6, $entity_full_name);
    $sth_pg->bind_param(7, $entity_description);
    $sth_pg->bind_param(8, $stp_operating_capacity_dir1);
    $sth_pg->bind_param(9, $stp_operating_capacity_dir2);
    $sth_pg->bind_param(10, $stp_flowgate_name);
    $sth_pg->bind_param(11, $plant_nameplate);
    $sth_pg->bind_param(12, $plant_negative_capacity);
    $sth_pg->bind_param(13, $latitude);
    $sth_pg->bind_param(14, $longitude);
    $sth_pg->bind_param(15, $plant_city);
    $sth_pg->bind_param(16, $plant_county);
    $sth_pg->bind_param(17, $plant_state_id);
    $sth_pg->bind_param(18, $plant_zip);
    $sth_pg->bind_param(19, $plant_code);
    $sth_pg->bind_param(20, $energy_source_id);
    $sth_pg->bind_param(21, $nerc_region_id);
    $sth_pg->bind_param(22, $time_stamp);
    $sth_pg->bind_param(23, $online_date);
    $sth_pg->bind_param(24, $stp_primary_direction);
    $sth_pg->bind_param(25, $stp_origin_type);
    $sth_pg->bind_param(26, $stp_origin);
    $sth_pg->bind_param(27, $stp_terminus_type);
    $sth_pg->bind_param(28, $stp_terminus);
    $sth_pg->bind_param(29, $active_ind);
    $sth_pg->bind_param(30, $control_area);
    $sth_pg->bind_param(31, $heat_rate);
    $sth_pg->bind_param(32, $country_code);
    $sth_pg->bind_param(33, $region_id);
    $sth_pg->bind_param(34, $angle);

    #print "$entity_id\n";
    
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

# Send notification that there were insert errors
if ( ($rows gt 0 && $rows ne $insert_count) || $insert_error_flag ne 0) {
    $error_message = "Expected Rows = $rows : Inserted_Rows = $insert_count: $insert_error_message";
    send_error_notification($error_message);
}

# END: entity table refresh


print "END: " . `date` . "\n";
system("$tracker_script $scriptname $tracker_number $tracker_hostname END");
