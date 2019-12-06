#!/usr/bin/perl
# 
# check_barcode_cgi.pl .cgi
#
print "Content-type:text/html\n\n";

$first_run = 0;
$error_flag = 0;
$buffer = $ENV{QUERY_STRING};

# NOTE: add output file name generation code here
$output_filename = "/var/www/cgi-bin/check_barcode/buffer.txt";

# Parse the url query values
@pairs = split(/&/, $buffer);
foreach $pair (@pairs) {
	($name, $value) = split(/=/, $pair);
	$value =~ tr/+/ /;
	$value =~ s/%([a-fA-F0-9][a-fA-F0-9])/pack("C", hex($1))/eg;
	$value =~ s/\n/ /g;	 # added to strip line breaks 
 	$value =~ s/\r//g;
	$value =~ s/\cM//g;
	$FORM{$name} = $value;
}


# print page title 
print "<html><head><title>Check Barcode</title></head><body 
bgcolor='white'><blockquote>\n";

# print page name
print "<big><font size=\"+3\"><big>Readers Digest Whitemail Returns 
Menu</big></font></big>" . "\n";
print "<br><br>";


# Don't do error check if query string is null
if(length($ENV{QUERY_STRING}) > 0) {

	# check for 16 digits
	if(length($FORM{"barcode"}) != 16 || $FORM{"barcode"} =~ /\D/) {
        	print "<b>\"" . $FORM{"barcode"} . "\" is <font 
color=\"#ff0000\">NOT</font> a <font color=\"#ff0000\">16 digit</font> number. Re-enter or Enter the 
next number or exit.</b>";
	$error_flag = 1;
	}

}


# code to check for duplicate numbers
# if the file does not exist skip the check as this will be the first time
# through thought
# if the file exists walk through looking for number. If number is matched
# stop, there is no need to check the number


if(-e $output_filename && $error_flag == 0 && length($ENV{QUERY_STRING}) > 0) {
	open(GOOD_BARCODES, "<$output_filename");
	while(<GOOD_BARCODES>) {
		if($_ =~ /$FORM{"barcode"}/ ) {
			$error_flag = 1;
        		print "<b>\"" . $FORM{"barcode"} . "\" is a <font 
color=\"#ff0000\">DUPLICATE</font> number. Go to the next or exit.</b>";
			close(GOOD_BARCODES);
			last;
		}
	}
	close(GOOD_BARCODES);
}	


# check whether this is initial invocation of program with no arguments yet.
if(length($ENV{QUERY_STRING}) == 0) {
	$first_run = 1;
        	print "<b>Enter barcode number. 16 digits only. Do NOT include 
letters.</b>";
} 

#######################################
# do check digit calculation
#######################################
if($error_flag == 0 && $first_run == 0) {
        # create second array to assign numbers to and intialize values to 0.
        @second_array = split(//, "0000000000000000");

        # put input number into array
        @input_barcode_array = split(//, $FORM{"barcode"});

        # grab last digit input. This is the check digit we need to test
        $input_barcode_check_digit = $input_barcode_array[15];


        # grab even number digits, do calculations, assign to second array
        for($i = 14, $even_tmp = 0; $i > -1; $i = $i - 2) {
                $even_tmp = $input_barcode_array[$i] * 2;
                if($even_tmp > 9) {
                        @even_tmp_array = split(//, $even_tmp);
                        $even_tmp = $even_tmp_array[0] + $even_tmp_array[1];
                }
                $second_array[$i] = $even_tmp;
        }

        # grab odd number digits, do calculations, assign to second array
        for($j = 13, $odd_tmp = 0; $j > 0; $j = $j - 2) {
                $odd_tmp = $input_barcode_array[$j] * 1;
                $second_array[$j] = $odd_tmp;
        }


        # add even and odd number and put in variable
        for($x = 0, $final_number = 0; $x < 16; $x++) {
                $final_number = $final_number + $second_array[$x];
        }

        # add $input_check_digit to $final_number, do mod 10,
        # if result is 0 we are good to go, print number out to file
        if(($input_barcode_check_digit + $final_number) % 10 == 0 ) {
		open(GOOD_BARCODES, ">>$output_filename");
		printf GOOD_BARCODES ("RDS%s|%s\n", $FORM{"barcode"} ,$FORM{"reason_code"});
		close(GOOD_BARCODES);
		print "<b>\"" . $FORM{"barcode"} . "\" is a <font 
color=\"#ff0000\">GOOD</font> number. Enter the next number or exit.</b>";
        } else {
		print "<b>\"" . $FORM{"barcode"} . "\" is a <font 
color=\"#ff0000\">BAD</font> number. Re-enter or Enter the next number or exit.</b>";
        }


} #end of if(error_flag == 0 etc)
#######################################

print "<form action=\"\/cgi-bin\/check_barcode\/check_barcode_cgi.pl\" 
method=GET>" . "\n"; 
print "<br>" . "\n"; 
print "<b>Barcode:<\/b> <input name=\"barcode\" size=16>" . "\n"; 



# print out the dropdown menu
print <<ReasonCode;
<b>Return Reason Code:</b> <select name="reason_code">   
<option value="13"> <b>Open:</b> Did Not Order 
<option value="14"> Open: Cancel 
<option value="15"> Open: Refused 
<option value="16"> Open: Deceased
<option value="23"> Closed: Did Not Order 
<option value="24"> Closed: Cancel 
<option value="25"> Closed: Refused
<option value="26"> Closed: Deceased
<option value="29"> Closed: Not Seen
</select>          
ReasonCode

print "<input type=\"submit\" value=\"Send\">" . "\n"; 
print "</form>" . "\n"; 

print <<EXIT;
<p><font face="Garamond"><font size=+2><a 
href="http://www.anetorderlv.net/">Exit</a></font></font></center>
EXIT
