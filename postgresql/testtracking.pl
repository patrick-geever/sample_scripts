#!/usr/bin/perl

# Source common ENV variables
do "/servers/config/gdr.env.pl";

use File::Basename;
$scriptname = basename ($0);
system("$tracker_script $scriptname $tracker_number $tracker_hostname START");

#----------------------------------------------

$output = `edb-psql -U $dbuser -h $dbhostname -p $dbport -d $dbname -c \"select current_user;\"`;
print "1 = $output\n";

@junk0 = `edb-psql -U $dbuser -h $dbhostname -p $dbport -d $dbname --tuples-only --quiet <<EOF | sed 's/^ //' | sed '$d'
-- connect reporting/gdrprd
select to_char(sysdate,'dd-mm-rrrr') from dual;
EOF`;
print "2 = @junk0\n";

$edbplusout = `edbplus $dbuser/$dbpassword\@$dbhostname:$dbport/$dbname <<EOF
-- connect reporting/gdrprd
select current_user;
EOF
`;
print "3 = $edbplusout\n";


#dbi
use DBI;
use DBD::Pg;

                # FOR MANUAL ERROR CHECKING- SETTING OFF THE ARGUMENTS
                my %attr = ( PrintError => 0, RaiseError => 0); 

		#NOTE: keep old oracle login information in the script as a comment 
		#my $dbh=DBI->connect("dbi:Oracle:gdrprod.company.com", "reporting", "gdrprod",\%attr)
		my $dbh=DBI->connect("DBI:Pg:dbname=$dbname;host=$dbhostname;port=$dbport", "$dbuser", "$dbpassword", \%attr)
		or die "Can't connect to Postgres database: $DBI::errstr\n";

                my $sql = qq{ select current_user };
                #my $sql = qq{ select datname from pg_database };
                my $sth = $dbh->prepare( $sql ) or die "failed to prepare\n";
                my $test = $sth->execute() or die "failed to execute\n";
                my $test2 = $sth->bind_columns(\$dbiout ) or die "failed to bind columns\n";
                while( $sth->fetch() ) {
			print "3 = $dbiout\n";
		}

# Email addresses or commands must be wrapped in if/else to make sure that emails to 
# production email addresses are only sent when script runs on the productions system 
if($mailflag eq 'GDRPRD') {
   $to_address = "GDROperations\@company.com";  
   @cc="";
   @bcc="";
} else {
   $to_address = $devmaillist;
   @cc = $devmaillist;
   @bcc = $devmaillist;
}

print "to_address = @to_address\n";

system("$tracker_script $scriptname $tracker_number $tracker_hostname END");

