#!/usr/bin/perl  -w

#
# check_idocs_sdq.pl
# Patrick Geever
# August 20, 1999
#
# version 4, Nov. 3, 1999 - fixed bug where detail lines were skipped
# version 5, Nov. 5, 1999 - fixed E2EDKA1001 line with existing ship to number
# version 6, March 19, 2002 - fixed field offsets for new SAP version 4.6c, 
#                             fields have been shifted 8 spaces to the right.


if(@ARGV != 1) {
    die "\nUsage: $0  source_file\n\n";
}


# Note:
# use getopts to parse command line arguments


stat($ARGV[0]) || die "Cannot open file: $ARGV[0]\n";

# change to sdq_perl working directory
chdir("/edi/sdq_perl");

# split the input file name to get actual filename from 
# command line that user a full path to the filename
@input_filename_array = split(/\//, $ARGV[0]);

# grab the last element of array, this is the filename
$input_filename = pop(@input_filename_array);


open(INPUT_FILE, $input_filename) || die;


$sdq_present_flag = 0;

# check for sdq segments in the input file
while(<INPUT_FILE>) {
    if($_ =~ /^Z2EDP01SDQ/) {
	$sdq_present_flag = 1;
	last;
    } 
}
close(INPUT_FILE);


# If there are no sdq segments just exit, otherwise, rename the file to 
# _$input_filename_ and then process the file
if($sdq_present_flag == 0) {
    exit 0;
} 
else {
    system("cp $input_filename backup/$input_filename.orig");
    rename("$input_filename", "_" . $input_filename . "_");
    $renamed_input_filename = "_" . $input_filename . "_";
}


open(RENAMED_INPUT_FILE, $renamed_input_filename) || die;

@empty_array = ();
$first_doc_flag = 0;

# this will be the original filename
open("OUTPUT_FILE", ">>$input_filename");

while(<RENAMED_INPUT_FILE>) {


    # first idoc
    if($_ =~ /^EDI_DC/ && $first_doc_flag == 0) {
	push(@array_of_strings, $_);
	$first_doc_flag++;
    }
    elsif($_ !~ /^EDI_DC/ ) {
	push(@array_of_strings, $_);
    }
    elsif($_ =~ /^EDI_DC/ && $first_doc_flag != 0) {

	# test for sdq segs
        # if no, print out idoc to OUTPUT_FILE
        # if yes, call sdq_splitter function and split & print out idocs 
        # to OUTPUT_FILE
	
	if(&sdq_check(@array_of_strings)) {

	    # call sdq splitter routine
	    &sdq_splitter(@array_of_strings);
	}
	else {
	    
	    print OUTPUT_FILE @array_of_strings;
	}

       	# zero out @array_of_strings for next idoc
	@array_of_strings = @empty_array;
	push(@array_of_strings, $_);

    } # end of elsif
    


    # process last idoc after grabbing last line in file



    if( eof(RENAMED_INPUT_FILE) ) {

#	    print "at eof\n";

	if(&sdq_check(@array_of_strings)) {
	    
#	    print "at eof sdq\n";
	    
	    # call sdq splitter routine
	    &sdq_splitter(@array_of_strings);
	}
	else {
	    print OUTPUT_FILE @array_of_strings;
	}

    } # end of if(eof)

} # end of while(<RENAMED_INPUT_FILE>)

close(RENAMED_INPUT_FILE);
close(OUTPUT_FILE);

# Note: to do 
# remove the renamed inputfile after we are done processing



########## subroutines ##################


sub sdq_check {

    # grab passed parameter
    @array_to_be_checked = @_;

    # walk through array and test each string for sdq_segs
    # if there are sdq_segs return 1
    # if no sdq_segs return 0
    foreach $idoc_string ( @array_to_be_checked ) {
	if($idoc_string =~ /^Z2EDP01SDQ/) {
	    return 1;
	}
    }
    return 0;
}


#################


sub sdq_splitter {

# grab passed parameter
my @sdq_array_of_strings= @_;


my $section_type_flag = 0;
my $header_line_count = 0;
my $header_string_length = 0;
my $line_item_count = 0;
my $line_item_line = 0;
my $ship_to_number = 0;
my $quantity = 0;
my $quantity_length = 0;
my $summary_line_count = 0;
my $begin_P01_line = "";
my $end_P01_line = "";
my $sdq_line = "";
my $ship_to_key = 0;
my $line_item = 0;
my $i = 0;
my $x = 0;
my $j = 0;

my %header_array = ();
my %line_item_array = ();
my %sdq_array = ();
my %summary_array = ();

# need to zero this out
my %ship_to_array = ();


foreach $sdq_line ( @sdq_array_of_strings ) {

    # set flag for section of idoc we are in
    # header = 1
    # line item = 2
    # sdq = 3
    # summary = 4

    if($sdq_line =~ /^EDI_DC/) {
	$section_type_flag = 1;
    }
    elsif($sdq_line =~ /^E2EDP01\d\d\d/) {
	$section_type_flag = 2;
	$line_item_count++;
    }
    elsif($sdq_line =~ /^Z2EDP01SDQ/) {
	$section_type_flag = 3;
	# zero out $line_item_line
	$line_item_line = 0;
    }
    elsif($sdq_line =~ /^E2EDS01/) {
	$section_type_flag = 4;
    }

    # store segments in appropriate arrays
    if($section_type_flag == 1) {

        # fix E2EDKA1001 (WE - ship to line) where line has an existing
        # ship to number already. If the line is MORE than 66 chars long,
	# that is 65 chars plus a carriage return, then truncate it to 65
	# chars and add the carriage return back before storing it.
	
	if($sdq_line =~ /WE/ && length($sdq_line) > 66) {
	    # truncate line
	    $sdq_line = substr("$sdq_line", 0, 65);
	    
	    # add carriage return
	    $sdq_line = sprintf("%s\n", $sdq_line);
	    $header_array{$header_line_count} = $sdq_line;
	} 
	else {
	    $header_array{$header_line_count} = $sdq_line;
	}
	$header_line_count++;
    }
    elsif($section_type_flag == 2) {
	$line_item_array{$line_item_count}{$line_item_line} = $sdq_line;
	$line_item_line++;
    }
    elsif($section_type_flag == 3) {
	# grab ship_to_number from line
	$ship_to_number = substr("$sdq_line", 221, 17);
	# remove any trailing newlines from ship_to_number
	chomp($ship_to_number);
	$sdq_array{$line_item_count}{$ship_to_number} = $sdq_line;
	$ship_to_array{$ship_to_number} = $ship_to_number;
       	$line_item_line = 0;  # reset counter for this
    }
    elsif($section_type_flag == 4) {
	$summary_array{$summary_line_count} = $sdq_line;
	$summary_line_count++;
    }

} # end of foreach 


foreach $ship_to_key (sort numerically (keys %ship_to_array)) {

    # print header lines
    for($i = 0; defined($header_array{$i}); $i++) {
	if($header_array{$i} =~ /WE/) {
	    chomp $header_array{$i};
	    $header_string_length = length($header_array{$i});
	    print OUTPUT_FILE $header_array{$i} . ' ' x (83 - $header_string_length) . $ship_to_key . "\n";
	}
	else {
	    print OUTPUT_FILE $header_array{$i};
	}
    }    
    
    # print out line item/quantity lines

    # go through all line_items to find any lines for this ship_to_key,
    # if defined print the line items, else if not defined, skip it and go 
    # to the next line_item, up to line_item_count

    for($line_item = 1; $line_item <= $line_item_count; $line_item++) {

	# if there is a line print it
	if(defined($sdq_array{$line_item}{$ship_to_key})) {

	    # grab quantity string from segment
	    $quantity = substr("$sdq_array{$line_item}{$ship_to_key}", 211, 10);

	    # format strings and insert quantity string
	    # quantity string starts at pos 74 and is 15 chars long
	    
	    # get beginning and end of quantity line
	    $begin_P01_line = substr($line_item_array{$line_item}{0}, 0, 74);
	    $end_P01_line = substr($line_item_array{$line_item}{0}, 89 , 282);
	    
	    # format quantity string
	    $quantity =~ s/\s.*\s//;    # remove spaces from quantity string
	    
	    $quantity_length = length($quantity); # get string length
	    $quantity = sprintf("%s%s", $quantity, ' ' x (15 - $quantity_length));
	    
	    # print line items with quantities inserted
	    print OUTPUT_FILE $begin_P01_line . $quantity . $end_P01_line;
	    
	
	    for($x = 1; defined($line_item_array{$line_item}{$x}); $x++) {
		print OUTPUT_FILE $line_item_array{$line_item}{$x};    	
	    }
	    
	}     # end of if(defined)
	# if there is no line item for this ship_to_key skip it
	else {
	    next;
	}
	
    }  # end of for line_item loop
    

    
    # print out the summary lines at end of new idoc
    for($j = 0; defined($summary_array{$j}); $j++) {
	print OUTPUT_FILE $summary_array{$j};
    }
    
}   # end of foreach ship_to_number loop


} # end of sdq_splitter subroutine


########################


sub numerically { $a <=> $b };

