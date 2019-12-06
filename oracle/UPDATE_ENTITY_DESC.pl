#!/usr/bin/perl

# Source common ENV variables
do "/servers/config/gdr.env.pl";

use File::Basename;
$scriptname = basename ($0);
system("$tracker_script $scriptname $tracker_number $tracker_hostname START");


$StartDate = `date`;
print "START: $StartDate\n";

use Date::Calc qw (:all);
use Date::Manip;

use DBI;
use DBD::Pg;

		# FOR MANUAL ERROR CHECKING- SETTING OFF THE ARGUMENTS
		my %attr = 
		(
			PrintError => 0,
			RaiseError => 0
		);
 
		# CREATING DATABASE HANDLE OR MAKING CONNECTION WITH ORACLE DATABASE
		# READ USERNAME AND PASSWORD FROM INI FILE
		
		
		
		# my $dbh=DBI->connect("dbi:Oracle:gdrprod.company.com", "master", "gdrprod",\%attr)
		my $dbh=DBI->connect("DBI:Pg:dbname=$dbname;host=$dbhostname;port=$dbport", "$dbuser", "$dbpassword", \%attr)
		or die "Can't connect to Postgres database: $DBI::errstr\n";

		my $test1 = $dbh->do("call master.UPDATE_T_ENTITY_DESC()") or
	       die "Can't execute sql $DBI::errstr\n";	
		
$dbh->disconnect();


$EndDate = `date`;
print "END: $EndDate\n";


system("$tracker_script $scriptname $tracker_number $tracker_hostname END");
