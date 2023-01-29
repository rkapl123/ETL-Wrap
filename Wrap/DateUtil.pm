package ETL::Wrap::DateUtil;

use strict;
use Time::Local; use Time::localtime; use Exporter; use POSIX qw(mktime);

our @ISA = qw(Exporter);
our @EXPORT = qw(%months %monate get_curdate get_curdatetime get_curdate_dot formatDate formatDateFromYYYYMMDD get_curdate_dash get_curdate_gen get_curdate_dash_plus_X_years get_curtime get_curtime_HHMM get_lastdateYYYYMMDD get_lastdateDDMMYYYY is_first_day_of_month is_last_day_of_month get_last_day_of_month weekday is_weekend is_holiday first_week first_weekYYYYMMDD last_week last_weekYYYYMMDD convertDate convertDateFromMMM convertDateToMMM convertToDDMMYYYY addDays addDaysHol addMonths subtractDays subtractDaysHol convertcomma convertToThousendDecimal get_dateseries parseFromDDMMYYYY parseFromYYYYMMDD convertEpochToYYYYMMDD);

our %months = ("Jan" => "01","Feb" => "02","Mar" => "03","Apr" => "04","May" => "05","Jun" => "06","Jul" => "07","Aug" => "08","Sep" => "09","Oct" => "10","Nov" => "11","Dec" => "12");
our %monate = ("Jan" => "01","Feb" => "02","Mär" => "03","Apr" => "04","Mai" => "05","Jun" => "06","Jul" => "07","Aug" => "08","Sep" => "09","Okt" => "10","Nov" => "11","Dez" => "12");

sub get_curdate {
	return sprintf("%04d%02d%02d",localtime->year()+ 1900, localtime->mon()+1, localtime->mday());
}

sub get_curdatetime {
	return sprintf("%04d%02d%02d_%02d%02d%02d",localtime->year()+1900,localtime->mon()+1,localtime->mday(),localtime->hour(),localtime->min(),localtime->sec());
}

sub get_curdate_dot {
	return sprintf("%02d.%02d.%04d",localtime->mday(), localtime->mon()+1, localtime->year()+ 1900);
}

sub formatDate {
	my ($y,$m,$d,$template) = @_;
	$template = "YMD" if !$template;
	my @months = ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec');
	my @monate = ('Jän', 'Feb', 'Mär', 'Apr', 'Mai', 'Jun', 'Jul', 'Aug', 'Sep', 'Okt', 'Nov', 'Dez');
	my $result = $template;
	$y = sprintf("%04d", $y);
	$m = sprintf("%02d", $m);
	$d = sprintf("%02d", $d);
	if ($result =~ /MMM/i) {
		my $mmm;
		$mmm = $months[$m-1] if $result =~ /MMM/;
		$mmm = $monate[$m-1] if $result =~ /mmm/;
		$result =~ s/MMM/$mmm/i;
	} else {
		$result =~ s/M/$m/;
	}
	$result =~ s/Y/$y/;
	$result =~ s/D/$d/;
	return $result;
}

sub formatDateFromYYYYMMDD {
	my ($year,$mon,$day) = $_[0] =~ /(.{4})(..)(..)/;
	my ($template) = $_[1];
	return formatDate($year,$mon,$day,$template);
}

sub get_curdate_gen {
	my ($template) = @_;
	return formatDate(localtime->year()+ 1900,localtime->mon()+1,localtime->mday(),$template);
}

sub get_curdate_dash {
	return sprintf("%02d-%02d-%04d",localtime->mday(), localtime->mon()+1, localtime->year()+ 1900);
}

sub get_curdate_dash_plus_X_years {
	my ($y) = $_[0];
	my ($year,$mon,$day) = $_[1] =~ /(.{4})(..)(..)/;
	my $daysToSubtract = $_[2] if $_[2];
	if ($year) {
		my $dateval;
		if ($daysToSubtract) {
			$dateval = localtime(timegm(0,0,12,$day,$mon-1,$year)-$daysToSubtract*24*60*60);
		} else {
			$dateval = localtime(timegm(0,0,12,$day,$mon-1,$year));
		}
		return sprintf("%02d-%02d-%04d",$dateval->mday(), $dateval->mon()+1, $dateval->year()+ 1900 + $y);
	} else {
		return sprintf("%02d-%02d-%04d",localtime->mday(), localtime->mon()+1, localtime->year()+ 1900 + $y);
	}
}

sub get_curtime {
	my ($format) = $_[0];
	$format = "%02d:%02d:%02d" if !$format;
	return sprintf($format,localtime->hour(),localtime->min(),localtime->sec());
}

sub get_curtime_HHMM {
	return sprintf("%02d%02d",localtime->hour(),localtime->min(),localtime->sec());
}

sub is_first_day_of_month {
	my ($y,$m,$d) = $_[0] =~ /(.{4})(..)(..)/;
	((gmtime(timegm(0,0,12,$d,$m-1,$y)-24*60*60))[4] != $m-1 ? 1 : 0);
}

sub is_last_day_of_month {
	my ($y,$m,$d) = $_[0] =~ /(.{4})(..)(..)/;
	my $hol = $_[1];
	# for respecting holidays add 1 day and compare month
	if ($hol) {
		my $shiftedDate = addDaysHol($_[0],1,"YMD",$hol);
		my ($ys,$ms,$ds) = $shiftedDate =~ /(.{4})(..)(..)/;
		($ms ne $m ? 1 : 0); 
	} else {
		((gmtime(timegm(0,0,12,$d,$m-1,$y) + 24*60*60))[4] != $m-1 ? 1 : 0);
	}
}
sub get_last_day_of_month {
	my ($y,$m,$d) = $_[0] =~ /(.{4})(..)(..)/;
	
	# first of following month minus 1 day is always last of current month, timegm expects 0 based month, $m is the following month for timegm therefore
	if ($m == 12) {
		# for December -> January next year
		$m = 0; # month 0 based
		$y++;
	}
	my $mon = (gmtime(timegm(0,0,12,1,$m,$y) - 24*60*60))[4]+1;
	my $day = (gmtime(timegm(0,0,12,1,$m,$y) - 24*60*60))[3];
	$y-- if $m == 0; # for December -> reset year again
	return sprintf("%04d%02d%02d",$y, $mon, $day);
}

sub weekday {
	my ($y,$m,$d) = $_[0] =~ /(.{4})(..)(..)/;
	(gmtime(timegm(0,0,12,$d,$m-1,$y)))[6]+1;
}

sub is_weekend {
	my ($y,$m,$d) = $_[0] =~ /(.{4})(..)(..)/;
	(gmtime(timegm(0,0,12,$d,$m-1,$y)))[6] =~ /(0|6)/;
}
# makeMD: argument in timegm form (datetime), returns date in format DDMM (for holiday calculation)
sub makeMD {
	sprintf("%02d%02d", (gmtime($_[0]))[3],(gmtime($_[0]))[4] + 1);
}

sub is_holiday {
	my ($hol) = $_[0];
	return 0 if $hol eq "WE";
	unless ($hol =~ /^WE$|^BS$|^BF$|^AT$|^TG$|^UK$/) {
		warn("calender <$hol> not implemented !");
		return 0;
	}
	my ($y,$m,$d) = $_[1] =~ /(.{4})(..)(..)/;
	# fixes holidays
	my $fixedHol = {"BS"=>{"0101"=>1,"0601"=>1,"0105"=>1,"1508"=>1,"2610"=>1,"0111"=>1,"0812"=>1,"2412"=>1,"2512"=>1,"2612"=>1},
					"BF"=>{"0101"=>1,"0601"=>1,"0105"=>1,"1508"=>1,"2610"=>1,"0111"=>1,"0812"=>1,"2412"=>1,"2512"=>1,"2612"=>1},
					"AT"=>{"0101"=>1,"0601"=>1,"0105"=>1,"1508"=>1,"2610"=>1,"0111"=>1,"0812"=>1,"2512"=>1,"2612"=>1},
					"TG"=>{"0101"=>1,"0105"=>1,"2512"=>1,"2612"=>1},
					"UK"=>{"0101"=>1,"2512"=>1,"2612"=>1}};
	# easter, first find easter sunday
	my $D = (((255 - 11 * ($y % 19)) - 21) % 30) + 21;
	my $easter = timegm(0,0,12,1,2,$y) + ($D + ($D > 48 ? 1 : 0) + 6 - (($y + int($y / 4) + $D + ($D > 48 ? 1 : 0) + 1) % 7))*86400;
	# then the rest
	my $goodfriday=makeMD($easter-2*86400);
	my $easterMonday=makeMD($easter+1*86400);
	my $ascensionday=makeMD($easter+39*86400);
	my $whitmonday=makeMD($easter+50*86400);
	my $corpuschristiday=makeMD($easter+60*86400);
	# enter as required for calendar
	my $easterHol = {"BS"=>{$easterMonday=>1,$ascensionday=>1,$whitmonday=>1,$corpuschristiday=>1,$goodfriday=>1},
					 "BF"=>{$easterMonday=>1,$ascensionday=>1,$whitmonday=>1,$corpuschristiday=>1},
					 "AT"=>{$easterMonday=>1,$ascensionday=>1,$whitmonday=>1,$corpuschristiday=>1},
					 "TG"=>{$easterMonday=>1,$goodfriday=>1},
					 "UK"=>{$easterMonday=>1,$goodfriday=>1}};
	# British specialties
	my $specialHol = 0;
	$specialHol = (first_week($d,$m,$y,1,5) || last_week($d,$m,$y,1,5) || last_week($d,$m,$y,1,8)) if ($hol eq "UK");
	if ($fixedHol->{$hol}->{$d.$m} or $easterHol->{$hol}->{$d.$m} or $specialHol) {
		1;
	} else {
		0;
	}
}

sub last_week {
	my ($d,$m,$y,$day,$month) = @_;
	$month = $m if !$month;
	unless ((0 <= $day) && ( $day <= 6)) {
		warn("day <$day> is out of range 0 - 6  (sunday==0)");
		return 0;
	}
	my $date = localtime(timelocal(0,0,12,$d,$m-1,$y));
	return 0 unless $m == $month; # return unless the month matches
	return 0 unless $date->wday() == $day; # return unless the (week)day matches
	return 0 unless (localtime(timelocal(0,0,12,$d,$m-1,$y)+7*24*60*60))->mon() != $m-1; # return unless 1 week later we're in a different month
	return 1;
}

sub last_weekYYYYMMDD {
	my ($date,$day,$month) = @_;
	my ($y,$m,$d) = $date =~ /(.{4})(..)(..)/;
	$month = $m if !$month;
	return last_week ($d,$m,$y,$day,$month);
}

sub first_week {
	my ($d,$m,$y,$day,$month) = @_;
	$month = $m if !$month;
	unless ((0 <= $day) && ( $day <= 6)) {
		warn("day <$day> is out of range 0 - 6  (sunday==0)");
		return 0;
	}
	my $date = localtime(timelocal(0,0,12,$d,$m-1,$y));
	return 0 unless $m == $month; # return unless the month matches
	return 0 if $d > 7; # can't be the first week of the month if day is after the 7th
	return 0 unless $date->wday() == $day; # return unless the (week)day matches
	return 0 unless (localtime(timelocal(0,0,12,$d,$m-1,$y)-7*24*60*60))->mon() != $m-1; # return unless 1 week earlier we're in a different month
	return 1;
}

sub first_weekYYYYMMDD {
	my ($date,$day,$month) = @_;
	my ($y,$m,$d) = $date =~ /(.{4})(..)(..)/;
	$month = $m if !$month;
	return first_week ($d,$m,$y,$day,$month);
}

sub convertDateFromMMM {
	my ($inDate, $day, $mon, $year) = @_;
	my ($d,$m,$y) = ($inDate =~ /(\d{2})-(\w{3})-(\d{4})/);
	my %months = ('Jan'=> 1, 'Feb'=> 2, 'Mar'=> 3, 'Apr'=> 4, 'May'=> 5, 'Jun'=> 6, 'Jul'=> 7, 'Aug'=> 8, 'Sep'=> 9, 'Oct'=> 10, 'Nov'=> 11, 'Dec'=> 12);
	$$day = $d;
	$$mon = $months{$m};
	$$year = $y;
	return sprintf("%02d.%02d.%04d",$d, $months{$m}, $y);
}

sub convertDateToMMM {
	my ($day,$mon,$year) = @_;
	my @months = ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec');
	return sprintf("%02d-%03s-%04d",$day, $months[$mon-1], $year);
}

sub convertToDDMMYYYY {
	my ($y,$m,$d) = $_[0] =~ /(.{4})(..)(..)/;
	return "$d.$m.$y";
}

sub addDays {
	my ($day,$mon,$year,$dayDiff) = @_;
	my $curDateEpoch = timelocal(0,0,0,$$day,$$mon-1,$$year-1900);
	my $diffDate = localtime($curDateEpoch + $dayDiff * 60 * 60 * 25);
	my @months = ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec');
	# dereference, so the passed variable is changed
	$$year = $diffDate->year+1900;
	$$mon = $diffDate->mon+1;
	$$day = $diffDate->mday;
	return sprintf("%02d-%03s-%04d",$$day, $months[$$mon-1], $$year);
}

sub subtractDays {
	my ($date,$days) = @_;
	my ($y,$m,$d) = $date =~ /(.{4})(..)(..)/;
	my $theDate = localtime(timelocal(0,0,12,$d,$m-1,$y) - $days*24*60*60);
	return sprintf("%04d%02d%02d",$theDate->year()+ 1900, $theDate->mon()+1, $theDate->mday());
}

sub subtractDaysHol {
	my ($date,$days,$template,$hol) = @_;
	$hol="AT" if !$hol;
	my ($y,$m,$d) = $date =~ /(.{4})(..)(..)/;
	# first subtract days
	my $refdate = localtime(timelocal(0,0,12,$d,$m-1,$y) - $days*24*60*60);
	# then subtract further days as long weekend or holidays
	if ($hol ne "NO") {
		while ($refdate->wday() == 0 || $refdate->wday() == 6 || is_holiday($hol, sprintf("%04d%02d%02d", $refdate->year()+1900, $refdate->mon()+1, $refdate->mday()))) {
			$refdate = localtime(timelocal(0,0,12,$refdate->mday(),$refdate->mon(),$refdate->year()+1900) - 24*60*60);
		}
	}
	return formatDate($refdate->year()+1900, $refdate->mon()+1, $refdate->mday(),$template);
}

sub addDaysHol {
	my ($date, $days, $template, $hol) = @_;
	$hol="AT" if !$hol;
	my ($y,$m,$d) = $date =~ /(.{4})(..)(..)/;
	# first add days
	my $refdate = localtime(timelocal(0,0,12,$d,$m-1,$y) + $days*24*60*60);
	# then add further days as long weekend or holidays
	if ($hol ne "NO") {
		while ($refdate->wday() == 0 || $refdate->wday() == 6 || is_holiday($hol,sprintf("%04d%02d%02d", $refdate->year()+1900, $refdate->mon()+1, $refdate->mday()))) {
			$refdate = localtime(timelocal(0,0,12,$refdate->mday(),$refdate->mon(),$refdate->year()+1900) + 24*60*60);
		}
	}
	return formatDate($refdate->year()+1900, $refdate->mon()+1, $refdate->mday(),$template);
}

sub addMonths {
	my ($date, $months, $template) = @_;
	my ($y,$m,$d) = $date =~ /(.{4})(..)(..)/;
	my $parts = localtime(timelocal(0,0,12,$d,$m-1,$y));
	@$parts[4] += $months; # increment months
	my $refdate =localtime(mktime @$parts);
	return formatDate($refdate->year()+1900, $refdate->mon()+1, $refdate->mday(),$template);
}

sub get_lastdateYYYYMMDD {
	my $refdate = time - 24*60*60;
	$refdate = time - 3*24*60*60 if (localtime->wday() == 1);
	return sprintf("%04d%02d%02d",localtime($refdate)->year() + 1900, localtime($refdate)->mon()+1, localtime($refdate)->mday());
}

sub get_lastdateDDMMYYYY {
	my $refdate = time - 24*60*60;
	$refdate = time - 3*24*60*60 if (localtime->wday() == 1);
	return sprintf("%02d.%02d.%04d",localtime($refdate)->mday(), localtime($refdate)->mon()+1, localtime($refdate)->year() + 1900);
}

sub convertcomma {
	my ($number, $divideBy) = @_;
	$number = $number / $divideBy if $divideBy;
	$number = "$number";
	$number =~ s/\./,/;
	return $number;
}

# converts $value into German format decimal separated by thousand divider
sub convertToThousendDecimal {
	my ($value,$ignoreDecimal) = @_;
	# get digits before decimal point and after (optionally divided by thousand separator ".")
	my ($intplaces,$decplaces) = $value =~ /(\d*)\.(\d*)/ if $value =~ /\./;
	if ($value !~ /\./) {
		$intplaces = $value;
		$decplaces = "0";
	}
	# converts digits before decimal point to thousand separated number
	my $quantity = reverse join '.', unpack '(A3)*', reverse $intplaces;
	$quantity = $quantity.($ignoreDecimal ? "" : ",".$decplaces);
	return $quantity;
}

sub get_dateseries {
	my ($fromDate,$toDate,$hol) = @_;
	my ($yf,$mf,$df) = $fromDate =~ /(.{4})(..)(..)/;
	my ($yt,$mt,$dt) = $toDate =~ /(.{4})(..)(..)/;
	my $from = timelocal(0,0,12,$df,$mf-1,$yf);
	my $to = timelocal(0,0,12,$dt,$mt-1,$yt);
	my @dateseries;
	for ($_= $from; $_<= $to; $_ += 24*60*60) {
		my $date = localtime($_);
		my $datestr = sprintf("%04d%02d%02d",$date->year()+1900,$date->mon()+1,$date->mday());
		if ($hol) {
			push @dateseries, $datestr if $date->wday() != 0 && $date->wday() != 6 && !is_holiday($hol,$datestr);
		} else {
			push @dateseries, $datestr;
		}
	}
	return @dateseries;
}

sub parseFromDDMMYYYY {
	my ($dateStr) = @_;
	my ($df,$mf,$yf) = $dateStr =~ /(..*)\.(..*)\.(.{4})/;
	return "invalid date" if !($yf >= 1900) or !($mf >= 1 && $mf <= 12) or !($df >= 1 && $df <= 31);
	return timelocal(0,0,0,$df,$mf-1,$yf);
}

sub parseFromYYYYMMDD {
	my ($dateStr) = @_;
	my ($yf,$mf,$df) = $dateStr =~ /(.{4})(..)(..)/;
	return "invalid date" if !($yf >= 1900) or !($mf >= 1 && $mf <= 12) or !($df >= 1 && $df <= 31);
	return timelocal(0,0,0,$df,$mf-1,$yf);
}

sub convertEpochToYYYYMMDD {
	my ($arg) = @_;
	if (ref($arg) eq 'Time::Piece') {
		return sprintf("%04d%02d%02d",$arg->year(),$arg->mon(),$arg->mday());
	} else {
		my $date = localtime($arg);
		return sprintf("%04d%02d%02d",$date->year()+1900,$date->mon()+1,$date->mday());
	}

}
1;
__END__
=head1 NAME

ETL::Wrap::DateUtil - Date and Time helping functions

=head1 SYNOPSIS

 %months = ("Jan" => "01","Feb" => "02","Mar" => "03","Apr" => "04","May" => "05","Jun" => "06","Jul" => "07","Aug" => "08","Sep" => "09","Oct" => "10","Nov" => "11","Dec" => "12");
 %monate = ("Jan" => "01","Feb" => "02","Mär" => "03","Apr" => "04","Mai" => "05","Jun" => "06","Jul" => "07","Aug" => "08","Sep" => "09","Okt" => "10","Nov" => "11","Dez" => "12");

 get_curdate ()
 get_curdatetime ()
 get_curdate_dot ()
 formatDate ($d, $m, $y, [$template])
 formatDateFromYYYYMMDD($date, [$template])
 get_curdate_gen ([$template])
 get_curdate_dash ()
 get_curdate_dash_plus_X_years ($years)
 get_curtime ()
 get_curtime_HHMM ()
 get_lastdateYYYYMMDD ()
 get_lastdateDDMMYYYY ()
 is_first_day_of_month ($date YYYYMMDD)
 is_last_day_of_month ($date YYYYMMDD, [$hol])
 get_last_day_of_month ($date YYYYMMDD)
 weekday ($date YYYYMMDD)
 is_weekend ($date YYYYMMDD)
 is_holiday ($hol, $date YYYYMMDD)
 first_week ($d,$m,$y,$day,[$month])
 first_weekYYYYMMDD ($date,$day,[$month])
 last_week ($d,$m,$y,$day,[$month])
 last_weekYYYYMMDD ($date,$day,[$month])
 convertDate ($date YYYY.MM.DD or YYYY/MM/DD)
 convertDateFromMMM ($inDate dd-mmm-yyyy, out $day, out $mon, out $year)
 convertDateToMMM ($day, $mon, $year)
 convertToDDMMYYYY ($date YYYYMMDD)
 addDays ($day, $mon, $year, $dayDiff)
 addDaysHol ($date, $days, [$template], $hol)
 addMonths ($date, $months, [$template])
 subtractDays ($date, $days)
 subtractDaysHol ($date, $days, [$template], $hol)
 convertcomma ($number, $divideBy)
 getInteractiveDate($daysback, [$template])
 convertToThousendDecimal($value, $ignoreDecimal)
 get_dateseries
 parseFromDDMMYYYY ($dateStr)
 parseFromYYYYMMDD ($dateStr)
 convertEpochToYYYYMMDD ($epoch)

=head1 DESCRIPTION

=item %months: conversion hash english months -> numbers, usage: $months{"Oct"} equals 10

=item %monate: conversion hash german months -> numbers, usage: $monate{"Okt"} equals 10

=item get_curdate: gets current date in format YYYYMMDD

=item get_curdatetime: gets current datetime in format YYYYMMDD_HHMMSS

=item get_curdate_dot: gets current date in format DD.MM.YYYY

=item formatDate: formats passed (arguments $y,$m,$d) into format as defined in $template

 $d .. day part
 $m .. month part
 $y .. year part
 $template .. optional, date template with D for day, M for month and Y for year (e.g. D.M.Y for 01.02.2016),
              D and M is always 2 digit, Y always 4 digit; if empty/nonexistent defaults to "YMD"
              special formats are MMM und mmm als monthpart, here three letter month abbreviations in englisch (MMM) or german (mmm) are returned as month

=item formatDateFromYYYYMMDD: Ergebnis: übergebenes datum (argument $datum) im format wie definiert in $template

 $datum .. datum im fromat YYYYMMDD
 $template .. optional, Datumsformatvorlage mit D für Tag, M für Monat und Y für Jahr (z.b. D.M.Y für 01.02.2016),
              D und M werden immer 2 stellig, Y wird immer vierstellig ersetzt; wenn leer/nicht vorhanden wird "YMD" angenommen. 
              Spezielle Formate sind MMM und mmm als monatsteil, hier werden dreistellige Monatskürzel englisch (MMM) bzw. deutsch (mmm) als monat zurückgegeben.

=item get_curdate_gen: Ergebnis: aktuelles datum im format wie definiert in $template

 $template .. optional, Datumsformatvorlage mit D für Tag, M für Monat und Y für Jahr (z.b. D.M.Y für 01.02.2016),
              D und M werden immer 2 stellig, Y wird immer vierstellig ersetzt; wenn leer/nicht vorhanden wird "YMD" angenommen.

=item get_curdate_dash: Ergebnis: aktuelles datum im format DD-MM-YYYY

=item get_curdate_dash_plus_X_years: Ergebnis: aktuelles datum + X jahre im format DD-MM-YYYY

 $y .. Jahre, die zum aktuellen oder angegebenen Datum dazugezählt werden sollen.
 $year,$mon,$day .. optionales Datum, von dem aus die X Jahre dazugezählt werden sollen (wenn nicht vorhanden, dann wird aktuelles genommen).
 $daysToSubtract .. Tage die noch vom Ergebnis abgezogen werden sollen (für pfadgenerator)

=item get_curtime: Ergebnis: aktuelle Zeit im format HH:MM:SS (oder wie in formatstring $format angegeben, es wird allerdings immer in der Reihenfolge HH MM SS formatiert)

 $format .. optionaler sprintf formatstring (zb %02d:%02d:%02d) für stunde, minute und sekunde

=item get_curtime_HHMM: Ergebnis: aktuelle Zeit im format HHMM

=item is_first_day_of_month: Ergebnis 1 wenn erster Tag des monats, 0 sonst

 $date .. datum im format YYYYMMDD


=item is_last_day_of_month: Ergebnis 1 wenn letzter Tag des monats, 0 sonst

 $date .. datum im format YYYYMMDD
 $hol .. optional, Kalender zur Berücksichtigung von Feiertagen beim Bestimmen des Monatsletzten

=item get_last_day_of_month: Gibt den letzten Tag des Monats des übergebenen Datums zurück

 $date .. datum im format YYYYMMDD

=item weekday: Ergebnis: 1..sonntag bis 7..samstag

 $date .. datum im format YYYYMMDD

=item is_weekend: Ergebnis 1 wenn samstag oder sonntag

 $date .. datum im format YYYYMMDD

=item is_holiday: Ergebnis 1 wenn Wochenende oder Feiertag

 $hol .. Feiertagskalender; aktuell unterstützt: BS (Bundesschatz freie Tage), BF (OeBFA freie Tage) AT, TG (Target), UK (siehe is_holiday) und WE (für "Nur-Wochenende").
         Wirft Fehler wenn Kalender nicht unterstützt (Hashlookup).
 $date .. datum im format YYYYMMDD

=item last_week: Ergebnis 1 wenn gegebenes Datum ($d,$m,$y) letzter gegebener wochentag ($day: Bereich 0 - 6, Sonntag==0) im gegebenen monat ($month)
 wenn $month nicht gegeben, wird dieses vom gegebenem Datum genommen.
 
 $d .. tagesteil
 $m .. monatsteil
 $y .. jahresteil
 $day .. gegebener wochentag
 $month .. optional, gegebenes monat

=item last_weekYYYYMMDD: Ergebnis 1 wenn gegebenes Datum ($date im Format YYYYMMDD) letzter gegebener wochentag ($day: Bereich 0 - 6, Sonntag==0) im gegebenen monat ($month)
 wenn $month nicht gegeben, wird dieses vom gegebenem Datum genommen.
 
 $date .. gegebenes Datum
 $day .. gegebener wochentag
 $month .. optional, gegebenes monat

=item first_week: Ergebnis 1 wenn gegebenes Datum ($d,$m,$y) erster gegebener wochentag ($day: Bereich 0 - 6, Sonntag==0) im gegebenen monat ($month)
 wenn $month nicht gegeben, wird dieses vom gegebenem Datum genommen.

 $d .. tagesteil
 $m .. monatsteil
 $y .. jahresteil
 $day .. gegebener wochentag
 $month .. optional, gegebenes monat

=item first_weekYYYYMMDD: Ergebnis 1 wenn gegebenes Datum ($date im Format YYYYMMDD) erster gegebener wochentag ($day: Bereich 0 - 6, Sonntag==0) im gegebenen monat ($month)
 wenn $month nicht gegeben, wird dieses vom gegebenem Datum genommen.

 $date .. gegebenes Datum
 $day .. gegebener wochentag
 $month .. optional, gegebenes monat
=item convertDate: konvertiert gegebenes datum auf format YYYYMMDD

 $date .. datum im format YYYY.MM.DD oder YYYY/MM/DD
sub convertDate ($) {
	my ($y,$m,$d) = ($_[0] =~ /(\d{4})[.\/](\d\d)[.\/](\d\d)/);
	return sprintf("%04d%02d%02d",$y, $m, $d);
}
=item convertDateFromMMM: konvertiert datum aus format dd-mmm-yyyy (01-Oct-05, englisch !), gibt datum im format DD.MM.YYYY ($day, $mon, $year werden dabei auch befüllt)

 $inDate .. datum, das konvertiert werden soll
 $day .. referenz für tagesteil
 $mon .. referenz für monatsteil 
 $year ..  referenz für jahresteil

=item convertDateToMMM : konvertiert datum in ($day, $mon, $year) auf format dd-mmm-yyyy (01-Oct-05, englisch !)

 $day .. tagesteil
 $mon .. monatsteil
 $year .. jahresteil
=item convertToDDMMYYYY: konvertiert datum in $datestring auf format dd.mm.yyyy

 $date .. datum im format YYYYMMDD

=item addDays : addiert $dayDiff zum datum ($day, $mon, $year) und gibt Ergebnis im Format dd-mmm-yyyy (01-Oct-05, englisch !) zurück
                Nebeneffekt: die argumente $day, $mon, $year werden ebenfalls verändert wenn sie nicht als literal sondern als variable angegeben wurden.

 $day .. tagesteil
 $mon .. monatsteil
 $year .. jahresteil
 $dayDiff .. tage, die addiert werden sollen

=item subtractDays: zieht $days Kalendertage von $date ab.

 $date .. datum im format YYYYMMDD
 $days .. Anzahl Kalendertage zum abziehen

=item subtractDaysHol: zieht $days Kalendertage von $date ab und berücksichtigt Wochenenden und Feiertage des mitgegebenen Kalenders.

 $date .. datum im format YYYYMMDD
 $days .. Anzahl Kalendertage zum abziehen
 $template .. optional, Datumsformatvorlage mit D für Tag, M für Monat und Y für Jahr (z.b. D.M.Y für 01.02.2016), 
              D und M werden immer 2 stellig, Y wird immer vierstellig ersetzt; wenn leer/nicht vorhanden wird "YMD" angenommen.
 $hol .. Feiertagskalender; aktuell unterstützt: NO (keine Berücksichtigung), WE (nur Wochenende), BS (Bundesschatz freie Tage), BF (OeBFA freie Tage) AT, TG (Target), UK (siehe is_holiday). Default wenn nicht gegeben = AT.

=item addDaysHol: addiert $days Kalendertage zu $date und berücksichtigt Wochenenden und Feiertage des mitgegebenen Kalenders.

 $date .. datum im format YYYYMMDD
 $days .. Anzahl Kalendertage zum hinzufügen
 $template .. optional, Datumsformatvorlage mit D für Tag, M für Monat und Y für Jahr (z.b. D.M.Y für 01.02.2016)
              D und M werden immer 2 stellig, Y wird immer vierstellig ersetzt; wenn leer/nicht vorhanden wird "YMD" angenommen.
 $hol .. Feiertagskalender; aktuell unterstützt: NO (keine Berücksichtigung), WE (nur Wochenende), BS (Bundesschatz freie Tage), BF (OeBFA freie Tage) AT, TG (Target), UK (siehe is_holiday). Default wenn nicht gegeben = AT.

=item addMonths: addiert $months Monate zu $date. Achtung bei Monatsenden (alles >28), wenn das Monatsende im Zielmonat nicht vorhanden ist, wird ins Folgemonat verschoben.

 $date .. datum im format YYYYMMDD
 $months .. Anzahl Monate zum hinzufügen
 $template .. optional, Datumsformatvorlage mit D für Tag, M für Monat und Y für Jahr (z.b. D.M.Y für 01.02.2016)
              D und M werden immer 2 stellig, Y wird immer vierstellig ersetzt; wenn leer/nicht vorhanden wird "YMD" angenommen.
=item get_lastdateYYYYMMDD: Ergebnis: letzter Geschäftstag (nur Wochenenden!) im format YYYYMMDD
=item get_lastdateDDMMYYYY: Ergebnis: letzter Geschäftstag (nur Wochenenden!) im format DDMMYYYY
=item convertcomma: konvertiert dezimalpunkt in $number auf komma, dividiert durch $divideBy vorher wenn $divideBy gesetzt

 $number ..  Zahl, die konvertiert werden soll
 $divideBy .. Zahl, durch die dividiert werden soll
=item getInteractiveDate: holt eine Datumseingabe vom Benutzer, Defaultdatum $daysback Geschäftstage in der Vergangenheit (ohne Feiertage), Rückgabewert im Format $template
 
 $daysback .. Tage, die für die Einholung des default Datums (Vorgabe, kann mit Enter durchquittiert werden) rückwärts gegangen werden soll (0 = heute, 1 gestern, ...).
 $template .. optional, Datumsformatvorlage mit D für Tag, M für Monat und Y für Jahr (z.b. D.M.Y für 01.02.2016)
              D und M werden immer 2 stellig, Y wird immer vierstellig ersetzt; wenn leer/nicht vorhanden wird "YMD" angenommen.
=item convertToThousendDecimal: konvertiert $value in Tausendertrennzeichen getrennte Dezimalzahl (deutsches format)
 
 $value .. Zahl, die konvertiert werden soll
 $ignoreDecimal .. ohne Dezimalstellen

=item get_dateseries: gibt Datumswerte (Format YYYYMMMDD) beginnend mit $fromDate bis $toDate zurück,
  wenn ein in $hol optional angegebener Feiertagskalender gesetzt ist, werden diese (inkl. Wochenende) berücksichtigt.
 
 $fromDate .. Beginndatum
 $toDate .. Enddatum
 $hol .. Feiertagskalender

=item parseFromDDMMYYYY: erzeugt time epoch aus Datumsstring (dd.mm.yyyy)

 $dateStr .. Datumsstring
=item parseFromYYYYMMDD: erzeugt time epoch aus Datumsstring (yyyymmdd)

 $dateStr .. Datumsstring

=item convertEpochToYYYYMMDD: erzeugt Datumsstring (yyyymmdd) aus epoche/Time::piece

 $arg .. Datum entweder als epoche (sekunden seit 1.1.1970) oder als Time::piece objekt

=head1 COPYRIGHT

Copyright (c) 2022 Roland Kapl

All rights reserved.  This program is free software; you can
redistribute it and/or modify it under the same terms as Perl itself.

The full text of the license can be found in the LICENSE file included
with this module.

=cut