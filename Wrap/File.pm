package ETL::Wrap::File;

use strict; 
use Text::CSV; use Data::XLSX::Parser; use Spreadsheet::ParseExcel; use Spreadsheet::WriteExcel; use Excel::Writer::XLSX; use Exporter; use Cwd;
use Log::Log4perl qw(get_logger); use Time::localtime; use Data::Dumper; use XML::LibXML; use Scalar::Util qw(looks_like_number); use XML::LibXML::Debugging;
use Encode; use ETL::Wrap::DateUtil; 

our @ISA = qw(Exporter);
our @EXPORT = qw(readText readExcel readXML writeText writeExcel);

# read text files
sub readText {
	my ($File, $process, $filenames) = @_;
	my $logger = get_logger();
	my @filenames = @{$filenames} if $filenames;
	my $redoSubDir = $process->{redoDir}."/" if $process->{redoFile};
	my $lineProcessing = $File->{lineCode};
	my $addtlProcessingTrigger = $File->{addtlProcessingTrigger};
	my $addtlProcessing = $File->{addtlProcessing};
	my $firstLineProc = $File->{firstLineProc};

	# read format configuration
	my ($poslen, $isFixLen, $skip, $sep); 
	my (@header, @targetheader);
	$skip = $File->{format_skip} if $File->{format_skip};
	$sep = $File->{format_sep} if $File->{format_sep};
	my $origsep = $sep;
	if ($sep =~ /^fix/) {
		# positions/length definitions from poslen definition: e.g. "poslen => [(0,3),(3,3)]"
		$poslen =  $File->{format_poslen};
		$sep = ";";
		$isFixLen = 1;
	} else {
		if (!$sep) {
			$logger->error("no separator set in ".Dumper($File)) ;
			return 0;
		}
	}
	@header = split $sep, $File->{format_header} if $File->{format_header};
	@targetheader = split $sep, $File->{format_targetheader} if $File->{format_targetheader};
	@targetheader = @header if !@targetheader; # if no specific targetheader defined use header instead
	$Data::Dumper::Terse = 1;
	$logger->debug("skip:$skip,sep:".Data::Dumper::qquote($origsep).",header:@header\ntargetheader:@targetheader\nlineProcessing:".$File->{LineCode}."\naddtlProcessingTrigger:".$File->{addtlProcessingTrigger}."\naddtlProcessing:".$File->{addtlProcessing}."\nfirstLineProc:".$firstLineProc);
	$Data::Dumper::Terse = 0;

	# read all files with same format
	for my $filename (@filenames) {
		open (FILE, "<".$File->{encoding}, $redoSubDir.$filename) or do { #
			if (! -e $redoSubDir.$filename) {
				$logger->error("no file $redoSubDir$filename to process...") unless ($File->{optional});
				$logger->warn("no file $redoSubDir$filename found... ");
			} else {
				$logger->error("file open error: $!")
			}
			return 0;
		};
		my $sv = Text::CSV->new ({
			binary    => 1,
			auto_diag => 1,
			sep_char  => $sep,
			eol => ($File->{format_eol} ? $File->{format_eol} : $/),
		});
		# local context for special line record separator
		{
			my $newRecSep;
			if ($File->{format_allowLinefeedInData}) {
				# enable binmode and set line record separator to CRLF, so line feeds in values don't create artificial new lines/records
				binmode(FILE, ":raw".$File->{encoding}); # raw so not to swallow CRLF
				$newRecSep = "\015\012";
				$logger->debug("binmode");
			}
			# change record separator (standard CRLF), if needed
			local $/ = $newRecSep if $newRecSep;
			my @layers = PerlIO::get_layers(FILE);
			$logger->debug("layers: @layers");
			$logger->debug("starting reading file $redoSubDir$filename ... ");
			if ($firstLineProc) {
				$_ = <FILE>;
				$_ = encode('cp1252', $_) if $File->{encoding};
				eval $firstLineProc;
				$logger->error("eval firstLineProc: ".$firstLineProc.$@) if ($@);
				$logger->debug("evaled: ".$firstLineProc);
			}
			if ($skip) {
				$skip-- if $firstLineProc; # if consumed already by firstLineProc skip one row less
				$logger->debug("skipping ".($skip =~ /^\d+$/ ? " $skip lines" : "until line contains $skip (inclusive)"));
				# skip first $skip rows in file (e.g. report header) if $skip is an integer, if $skip is non-integer, skip until the text $skip appears (inclusive)
				if ($skip =~ /^\d+$/) {
					for (1 .. $skip) {$_ = <FILE>};
				} else {
					while (<FILE>) {
						$_ = encode('cp1252', $_) if $File->{encoding};
						last if /$skip/;
					}
				}
			}

			# iterate through all rows of file
			my $lineno = 0;
			my (@line,@previousline);
LINE:
			while (<FILE>) {
				$_ = encode('cp1252', $_) if $File->{encoding};
				chomp;
				# in case lineProcessing or addtlProcessing needs access to whole row -> $rawline
				my $rawline = $_;
				# skip empty rows
				next LINE if $_ eq "";
				@previousline = @line;
				if ($isFixLen) {
					@line = undef;
					for (my $i=0;$i<@header;$i++) {
						$line[$i] = substr ($_, $poslen->[$i][0],$poslen->[$i][1]-$poslen->[$i][0]);
					}
				} else {
					if ($File->{format_quotedcsv}) {
						if ($sv->parse($_)) {
							@line = $sv->fields();
						} else {
							$logger->error("couldn't parse quoted csv row: ".$sv->error_diag());
						}
					} else {
						@line = split $sep;
					}
				}
				$lineno++;
				next LINE if $line[0] eq "" and !$lineProcessing;
				readRow($process,\@line,\@previousline,\@header,\@targetheader,$rawline,$lineProcessing,$addtlProcessingTrigger,$addtlProcessing,$File->{format_thousandsep},$File->{format_decimalsep},$lineno);
			}
		}
		close FILE;
		if (!$process->{data} and !$File->{emptyOK}) {
			$logger->error("Empty file: $filename, no data returned !!");
			return 0;
		}
	}
	$logger->trace("amount of rows:".scalar(@{$process->{data}})) if $logger->is_trace;
	$logger->trace(Dumper($process->{data})) if $logger->is_trace;
	return 1;
}

# global variables for excel parsing
my $startRow; # starting row (header)
my %dateColumn; # lookup for columns with date values (key: excel column, numeric, starting with 1, value: 1 (boolean))
my %headerColumn; # lookup for header (key: excel column, numeric, starting with 1, actual column of header field, value: 1 (boolean))
my $worksheet; # worksheet to be read, old format (numeric, starting with 1)
my %dataRows; # intermediate storage for row values
my $maxRow; # bottom most row
my %xlheader; # expected excel headers (key: excel column/numeric, starting with 1, value: expected content of header in excel)
my $stoppedOnEmptyValue; 
my $stopOnEmptyValueColumn;

# event handler for readExcel (xls format)
sub cell_handler {
	my $workbook    = $_[0];
	# for the Spreadsheet::ParseExcel index, rows and columns are 0 based, generally semantics is 1 based
	my $sheet_index = $_[1]+1;
	my $row         = $_[2]+1;
	my $col         = $_[3]+1;
	my $cell        = $_[4];
	my $logger = get_logger();
	return unless $sheet_index eq $worksheet; # only parse desired worksheet
	if ($row == $startRow && $headerColumn{$col}) {
		# check header row here as well
		$logger->error("expected header '".$xlheader{$col}."' not in column ".$col.", instead got: ".$cell->unformatted()) if $xlheader{$col} ne $cell->unformatted();
	} elsif ($headerColumn{$col}) {
		if (($stopOnEmptyValueColumn eq $col && !$cell) || $stoppedOnEmptyValue) {
			$logger->warn("empty cell in row $row / column $col and stopOnEmptyValueColumn is set to $col, skipping from here now") if !$stoppedOnEmptyValue; # pass warning only once
			$stoppedOnEmptyValue = 1;
		} else { # data row
			if ($dateColumn{$col}) {
				# with date values need value(), otherwise (unformatted) a julian date (decimal representing date and time) is returned
				# parse from US date format into YYYYMMDD, time parts are still ignored!
				if ($cell) {
					my ($m,$d,$y) = ($cell->value() =~ /(\d+?)\/(\d+?)\/(\d{4})/);
					$dataRows{$row}{$headerColumn{$col}} = sprintf("%04d%02d%02d",$y,$m,$d);
				}
			} else {
				# non date values are fetched unformatted
				$dataRows{$row}{$headerColumn{$col}} = $cell->unformatted() if $cell;
			}
			$maxRow = $row if $maxRow < $row;
			#$logger->info(Dumper($cell));
			#my $stopHere = <STDIN>; # for step debugging, uncomment these 2 lines
		}
	}
}

# event handler for readExcel (xlsx format)
sub row_handlerXLSX {
	my $rowDetails = $_[1];
	my $logger = get_logger();
	for my $cellDetail (@$rowDetails) {
		my $row = $cellDetail->{"row"};
		my $col = $cellDetail->{"c"};
		my $value = $cellDetail->{"v"};

		if ($row == $startRow && $headerColumn{$col}) {
			# check header row here as well
			$logger->error("expected header '".$xlheader{$col}."' not in column ".$col.", instead got: $value") if $xlheader{$col} ne $value;
		} elsif ($headerColumn{$col}) {
			if (($stopOnEmptyValueColumn eq $col && !$value) || $stoppedOnEmptyValue) {
				$logger->warn("empty cell in row $row / column $col and stopOnEmptyValueColumn is set to $col, skipping from here now") if !$stoppedOnEmptyValue; # pass warning only once
				$stoppedOnEmptyValue = 1;
			} else { # data row
				$logger->trace($headerColumn{$col}.":\n".Dumper($cellDetail)) if $logger->is_trace;
				if ($dateColumn{$col}) {
					# date fields are converted from epoch format !
					$dataRows{$row}{$headerColumn{$col}} = convertEpochToYYYYMMDD($value);
				} else {
					# non date values taken directly
					$dataRows{$row}{$headerColumn{$col}} = $value;
				}
				$maxRow = $row if $maxRow < $row;
			}
		}
	}
}

# read Excel file (format depends on setting)
sub readExcel {
	my ($File, $process, $filenames) = @_;
	my $logger = get_logger();
	$stopOnEmptyValueColumn = $File->{format_stopOnEmptyValueColumn};
	$stoppedOnEmptyValue = 0; # reset
	my @filenames = @{$filenames} if $filenames;
	my $redoSubDir = $process->{redoDir}."/" if $process->{redoFile};
	my $lineProcessing = $File->{lineCode};
	my $addtlProcessingTrigger = $File->{addtlProcessingTrigger};
	my $addtlProcessing = $File->{addtlProcessing};
	
	# reset module global header configs
	%dateColumn = undef;
	%headerColumn = undef;
	%xlheader=  undef;

	# read format configuration
	my (@header, @targetheader);
	my $sep = "\t";
	$startRow += $File->{format_skip} if $File->{format_skip};
	$logger->error("no header defined") if !$File->{format_header};
	$logger->error("no targetheader defined") if !$File->{format_targetheader};
	@header = split $sep, $File->{format_header};
	@targetheader = split $sep, $File->{format_targetheader};
	$logger->debug("skip: ". $File->{format_skip}.", header: @header \ntargetheader: @targetheader \nlineProcessing:".$File->{LineCode}."\naddtlProcessingTrigger:".$File->{addtlProcessingTrigger}."\naddtlProcessing:".$File->{addtlProcessing});
	# prepare date field lookup
	if ($File->{format_dateColumns}) {
		for my $col (@{$File->{format_dateColumns}}) {
			$dateColumn{$col} = 1;
		}
	}
	# prepare column lookups 
	# headerColumn: needed target column -> target field name
	# xlheader: original column name -> expected content in header cell
	my $i=0;
	if ($File->{format_headerColumns} and ref($File->{format_headerColumns}) eq "ARRAY") {
		$logger->debug("reading header definitions from format_headerColumns");
		if (@{$File->{format_headerColumns}} != @header or @{$File->{format_headerColumns}} != @targetheader) {
			$logger->error("format_headerColumns has different length than format_header or format_targetheader definitions");
			return 0;
		}
		for my $col (@{$File->{format_headerColumns}}) {
			$headerColumn{$col} = $targetheader[$i];
			$xlheader{$col} = $header[$i];
			$i++;
		}
	} else {
		$logger->debug("no format_headerColumns given, reading header definitions directly from \@header assuming simple list starting with column 1 having \@header length columns");
		for (@header) {
			$headerColumn{$i+1} = $targetheader[$i];
			$xlheader{$i+1} = $header[$i];
			$i++;
		}
	}
	@header = @targetheader; # in the end only target header is important
	
	# read all files with same format
	for my $filename (@filenames) {
		# reset module global variables
		$startRow = 1; 
		%dataRows = undef;
		$maxRow = 1;

		# check excel file existence
		if (! -e $redoSubDir.$filename) {
			$logger->error("no excel file ($filename) to process: $!") unless ($File->{optional});
			$logger->warn("no file $redoSubDir$filename found"); 
			return 0;
		}

		# read in excel file
		my $parser;
		if ($File->{format_xlformat} =~ /^xlsx$/i) {
			$logger->debug("open xlsx file $redoSubDir$filename ... ");
			$parser = Data::XLSX::Parser->new;
			$parser->open($redoSubDir.$filename);
			$parser->add_row_event_handler(\&row_handlerXLSX);

			if ($File->{format_worksheet}) {
				$worksheet = $parser->workbook->sheet_id($File->{format_worksheet});
				$logger->logdie("no worksheet found named ".$File->{format_worksheet}.", maybe try {format_worksheetID} (numerically ordered place)") if !$worksheet;
			} elsif ($File->{format_worksheetID}) {
				$worksheet = $File->{format_worksheetID};
			} else {
				$logger->logdie("neither worksheetname nor worksheetID (numerically ordered place) given");
			}
			$logger->debug("starting parser for xlsx sheet name: ".$File->{format_worksheet}.", id:".$worksheet);
			$parser->sheet_by_id($worksheet);
		} elsif ($File->{format_xlformat} =~ /^xls$/i) {
			$logger->warn("worksheets can't be found by name for the old xls format, please pass numerically ordered place in {format_worksheetID}") if ($File->{format_worksheet});
			$worksheet = $File->{format_worksheetID} if $File->{format_worksheetID};
			$logger->debug("starting parser for xls file $redoSubDir$filename ... ");
			$parser = Spreadsheet::ParseExcel->new(
				CellHandler => \&cell_handler,
				NotSetCell  => 1
			);
			my $workbook = $parser->parse($redoSubDir.$filename);
			if ( !defined $workbook ) {
				$logger->error("excel parsing error: ".$parser->error());
				return 0;
			}
		} else {
			$logger->error("unrecognised excel format passed in \$File->{format_xlformat}:".$File->{format_xlformat});
			return 0;
		}

		# iterate rows
		my (@line,@previousline);
LINE:
		for my $lineno ($startRow+1 .. $maxRow) {
			@previousline = @line;
			@line = undef;
			# get @line from stored values
			for (my $i = 0; $i < @header; $i++) {
				$line[$i] = $dataRows{$lineno}{$header[$i]};
			}
			readRow($process,\@line,\@previousline,\@header,\@targetheader,undef,$lineProcessing,$addtlProcessingTrigger,$addtlProcessing,$File->{format_thousandsep},$File->{format_decimalsep},$lineno);
		}
		close FILE;
		if (scalar(@{$process->{data}}) == 0 and !$File->{emptyOK}) {
			$logger->error("Empty file: $filename, no data returned !!");
			return 0;
		}
	}
	$logger->trace("amount of rows: ".scalar(@{$process->{data}})) if $logger->is_trace;
	$logger->trace(Dumper($process->{data})) if $logger->is_trace;
	return 1;
}

# read XML file
sub readXML {
	my ($File, $process, $filenames) = @_;
	my $logger = get_logger();
	my @filenames = @{$filenames} if $filenames;
	my $redoSubDir = $process->{redoDir}."/" if $process->{redoFile};
	my $lineProcessing = $File->{lineCode};
	my $addtlProcessingTrigger = $File->{addtlProcessingTrigger};
	my $addtlProcessing = $File->{addtlProcessing};

	# read format configuration
	my (@header, @targetheader);
	my $sep = $File->{format_sep};
	if (!$sep) {
		$logger->error("no separator set in ".Dumper($File));
		return 0;
	}
	$logger->error("no header defined") if !$File->{format_header};
	@header = split $sep, $File->{format_header};
	$logger->debug("header: @header \nlineProcessing:".$File->{LineCode}."\naddtlProcessingTrigger:".$File->{addtlProcessingTrigger}."\naddtlProcessing:".$File->{addtlProcessing});
	@targetheader = @header;

	# read all files with same format
	for my $filename (@filenames) {
		if (! -e $redoSubDir.$filename) {
			$logger->error("no XML file ($filename) to process") unless ($File->{optional});
			$logger->warn("file $redoSubDir$filename not found");
			return 0;
		}

		my $xmldata = XML::LibXML->load_xml(location => $redoSubDir.$filename, no_blanks => 1);
		my $xpc = XML::LibXML::XPathContext->new($xmldata);
		if (ref($File->{format_namespaces}) eq 'HASH') {
			$xpc->registerNs($_, $File->{format_namespaces}{$_}) for keys (%{$File->{format_namespaces}});
		}
		$logger->error("no format_xpathRecordLevel passed") unless ($File->{format_xpathRecordLevel});
		$logger->error("no format_fieldXpath hash passed") unless ($File->{format_fieldXpath} && ref($File->{format_fieldXpath}) eq 'HASH');
		$logger->trace("format_xpathRecordLevel: ".$File->{format_xpathRecordLevel}) if $logger->is_trace;
		$logger->trace("format_fieldXpath: ".Dumper($File->{format_fieldXpath})) if $logger->is_trace;
		my @records = $xpc->findnodes($File->{format_xpathRecordLevel});
		$logger->warn("no records found") if @records == 0;
		$logger->trace("total document content: ".$xpc->getContextNode->toClarkML()) if $logger->is_trace;
		# iterate through all rows of file
		my $lineno = 0;
		my (@line,@previousline);
		foreach my $record (@records) {
			@previousline = @line;
			@line = undef;
			# get @line from stored values
			if (ref($record) eq "XML::LibXML::Element") {
				$logger->trace("node content: ".$record->toClarkML()) if $logger->is_trace;
				my @headerColumns = keys (%{$File->{format_fieldXpath}});
				for (my $i = 0; $i < @headerColumns; $i++) {
					$logger->trace("field:".$header[$i].",\$File->{format_fieldXpath}{".$header[$i]."}:".$File->{format_fieldXpath}{$header[$i]}) if $logger->is_trace;
					if ($File->{format_fieldXpath}{$header[$i]} =~ /^\//) {
						# absolute paths -> leave context node and find in the root doc (no context node argument)
						$logger->trace("absolute fieldXpath:".$File->{format_fieldXpath}{$header[$i]}) if $logger->is_trace;
						$line[$i] = $xpc->findvalue($File->{format_fieldXpath}{$header[$i]});
					} else {
						# relative paths -> context node is current record node
						$logger->trace("relative fieldXpath:".$File->{format_fieldXpath}{$header[$i]}) if $logger->is_trace;
						$line[$i] = $xpc->findvalue($File->{format_fieldXpath}{$header[$i]}, $record);
					}
				}
			}
			$lineno++;
			readRow($process,\@line,\@previousline,\@header,\@targetheader,undef,$lineProcessing,$addtlProcessingTrigger,$addtlProcessing,$File->{format_thousandsep},$File->{format_decimalsep},$lineno);
		}
		if (!$process->{data} and !$File->{emptyOK}) {
			$logger->error("empty file: $filename, no data returned");
			return 0;
		}
	}
	return 1;
}

		# remove thousand separators for numerals based on configured thousand/decimal separator and change decimal separator to \d+\.?\d*
sub normalizeNumerics {
	my ($number,$thousandsep,$decimalsep) = @_;
	$number =~ s/$thousandsep//g if $number =~ /^-?\d{1,3}($thousandsep\d{3})+($decimalsep\d*)?$/;
	if ($decimalsep ne "\\.") {
		$number =~ s/$decimalsep/\./ if $number =~ /^-?\d+$decimalsep\d+$/ or $number =~ /^-*\d*$decimalsep?\d+E*[-+]*\d*$/;
	}
	return $number;
}

# read row into final line hash (including special "hook" code)
sub readRow {
	my ($process,$line,$previousline,$header,$targetheader,$rawline,$lineProcessing,$addtlProcessingTrigger,$addtlProcessing,$thousandsep,$decimalsep,$lineno) = @_;
	my @line = @$line;
	my @previousline = @$previousline;
	my @header = @$header;
	my @targetheader = @$targetheader;
	my $logger = get_logger();
	$thousandsep = "," if !$thousandsep; $decimalsep = "." if !$decimalsep;
	$logger->trace("line: @{$line},previousline: @{$previousline},header: @{$header},targetheader: @{$targetheader},rawline: $rawline, lineProcessing: $lineProcessing, addtlProcessingTrigger: $addtlProcessingTrigger, addtlProcessing: $addtlProcessing,thousandsep: $thousandsep,decimalsep: $decimalsep,lineno: $lineno") if $logger->is_trace;
	$thousandsep = "\\".$thousandsep; $decimalsep = "\\".$decimalsep;
	# if field is being replaced by a different name from targetheader, the data with the original name is placed in %templine (for further actions in $lineProcessing)
	# the final value is put in $line{$targetheader}.
	# there is also data from the previous line (%previousline) and the previous temp line (%previoustempline).
	my (%line,%templine,%previousline,%previoustempline);
	# iterate through fields of current row
	for (my $i = 0; $i < @line; $i++) {
		# first trim leading and trailing spaces
		$line[$i] =~ s/^ *//;
		$line[$i] =~ s/ *$//;
		$line[$i] = normalizeNumerics($line[$i],$thousandsep,$decimalsep);
		
		# only process as targetheader, if they are not the same as the original header (allows special access to original header via $templine/$previoustempline)
		if ($header[$i] ne $targetheader[$i]) {
			# prevent autovivification of hash entries, if $i is potentially > @header or > @targetheader
			$line{$targetheader[$i]} = $line[$i] if $targetheader[$i];
			$previousline{$targetheader[$i]} = $previousline[$i] if $targetheader[$i];
			$templine{$header[$i]} = $line[$i] if $header[$i];
			$previoustempline{$header[$i]} = $previousline[$i] if $header[$i];
		} else {
			$line{$header[$i]} = $line[$i] if $header[$i];
			$previousline{$header[$i]} = $previousline[$i] if $header[$i];
		}
		# additional (field)processing triggered (in addtlProcessingTrigger => "fieldname", addtlProcessing => "..."})
		if (($addtlProcessingTrigger eq "*" or $header[$i] eq $addtlProcessingTrigger) && $header[$i]) { # && $header[$i], as sometimes $header[$i] is empty/undefined
			$logger->trace('BEFORE: $header['.$i.']:'.$header[$i].',$line['.$i.']:'.$line[$i].",addtlProcessing:".$addtlProcessing) if $logger->is_trace;
			eval $addtlProcessing;
			$logger->error("eval addtlProcessing: $addtlProcessing".$@) if ($@);
			$logger->trace('AFTER: $header['.$i.']:'.$header[$i].',$line['.$i.']:'.$line[$i]."<<line: $lineno") if $logger->is_trace;
		}
	}
	# additional row processing defined
	if ($lineProcessing) {
		eval $lineProcessing;
		$logger->error("eval lineProcessing: $lineProcessing".$@) if ($@);
		if ($logger->is_trace) {
			$logger->trace("lineProcessing:".$lineProcessing.", line: $lineno");
			$logger->trace("templine:\n".Dumper(\%templine));
			$logger->trace("previousline:\n".Dumper(\%previousline));
			$logger->trace("previoustempline:\n".Dumper(\%previoustempline));
		}
	}
	$logger->trace("line:\n".Dumper(\%line)) if $logger->is_trace;
	push @{$process->{data}}, \%line if %line;
}

# write text file
sub writeText {
	my ($File, $process) = @_;
	my $logger = get_logger();
	
	my $filename = $File->{filename};
	my $data = $process->{data};
	if (ref($data) ne 'ARRAY') {
		$logger->error("passed data in \$process is not a ref to array:".Dumper($process));
		return 0;
	}
	# in case we need to print out csv/quoted values
	my $sv = Text::CSV->new ({
			binary    => 1,
			auto_diag => 1,
			sep_char  => $File->{format_sep},
			eol => ($File->{format_eol} ? $File->{format_eol} : $/),
		});

	my @columnnames; my @paddings;
	if (ref($File->{columns}) eq 'HASH') {
		@columnnames = map {$File->{columns}{$_}} sort keys %{$File->{columns}};
	} else {
		$logger->error("no field information given (columns should be ref to hash)");
		return 0;
	}
	if (ref($File->{format_padding}) eq 'HASH') {
		@paddings = map {$File->{format_padding}{$_}} sort keys %{$File->{format_padding}};
	} else {
		if ($File->{format_sep} eq "fix") {
			$logger->error("no padding information given for fixed length format (padding => ref to hash)");
			return 0;
		}
	}
	$logger->debug("fields: @columnnames");
	$logger->debug("paddings: @paddings");
	my $headerRow;
	my $col = 0; # iterate through @paddings in parallel.
	my $firstcol = 1;
	for my $colname (@columnnames) {
		if (!$File->{columnskip}{$colname}) {
			if ($File->{format_quotedcsv}) {
				push @$headerRow, $colname;
			} else {
				# first column has no separator before. if there is a special separator for heading, then use it, else the standard one
				$headerRow = $headerRow.($firstcol ? "" : ($File->{format_sepHead} ? $File->{format_sepHead} : $File->{format_sep})).$colname if ($File->{format_sep} ne "fix");
				$headerRow = $headerRow.sprintf("%-*s%s", $paddings[$col],$colname) if ($File->{format_sep} eq "fix");
				$firstcol = 0;
			}
		}
		$col++;
	}
	# open file for writing
	$logger->debug("writing to ".$filename);
	open (FHOUT, ">".$File->{encoding},$filename) or do {
		$logger->error("file creation error: $!");
		return 0;
	};
	# write header
	print FHOUT $File->{format_beforeHeader} if $File->{format_beforeHeader};
	unless ($File->{format_suppressHeader}) {
		if ($File->{format_quotedcsv}) {
			if (!$sv->print(\*FHOUT, $headerRow)) {
				$logger->error("error writing quoted csv header row: ".$sv->error_diag());
				return 0;
			}
		} else {
			print FHOUT $headerRow."\n";
		}
	}
	
	# write data
	$logger->trace("passed data:\n".Dumper($data)) if $logger->is_trace;
	for (my $i=0; $i<scalar(@{$data}); $i++) {
		# data row
		my $row = $data->[$i];
		my $lineRow;
		# chain all data in a row
		my $col = 0; $firstcol = 1;
		for my $colname (@columnnames) {
			if (!$File->{columnskip}{$colname}) {
				if (ref($row) ne "HASH") {
					$logger->error("row passed in (\$process->{data}) is no ref to hash! should be \$VAR1 = {'key' => 'value', ...}:\n".Dumper($row));
					return 0;
				}
				my $value = $row->{$colname};
				$logger->trace("\$value for \$colname $colname: $value") if $logger->is_trace;
				if ($File->{addtlProcessingTrigger} && $File->{addtlProcessing}) {
					eval $File->{addtlProcessingTrigger} if (eval $File->{addtlProcessingTrigger});
					if ($@) {
						$logger->error("error in eval addtlProcessing: ".$File->{addtlProcessingTrigger}.":".$@);
						return 0;
					}
				}
				if ($File->{format_quotedcsv}) {
					push @$lineRow, $value;
				} else {
					# last column ($columnnames[@columnnames-1]) should have not separator afterwards
					$lineRow = $lineRow.($firstcol ? "" : $File->{format_sep}).sprintf("%s", $value) if ($File->{format_sep} ne "fix");
					# additional padding for fixed length format
					$lineRow = $lineRow.sprintf("%-*s%s", $paddings[$col],$value) if ($File->{format_sep} eq "fix");
					$firstcol = 0;
				}
			}
			$col++;
		}
		if ($File->{format_quotedcsv}) {
			if (!$sv->print(\*FHOUT, $lineRow)) {
				$logger->error("error writing quoted csv row: ".$sv->error_diag());
				return 0;
			}
		} else {
			print FHOUT $lineRow."\n";
		}
		
		$logger->trace("row: ".$lineRow) if $logger->is_trace();
	}
	close FHOUT;
	return 1;
}

# write Excel file
sub writeExcel {
	my ($File, $process) = @_;
	my $logger = get_logger();
	
	my $data = $process->{data};
	if (ref($data) ne 'ARRAY') {
		$logger->error("passed data in \$process is not a ref to array:".Dumper($process));
		return 0;
	}

	my @columnnames;
	if (ref($File->{columns}) eq 'HASH') {
		@columnnames = map {$File->{columns}{$_}} sort keys %{$File->{columns}};
	} else {
		$logger->error("no field information given (columns should be ref to hash)");
		return 0;
	}

	my ($workbook,$worksheet);
	if ($File->{format_xlformat} =~ /^xls$/i) {
		$logger->debug("writing to xls format file ".$File->{filename});
		$workbook = Spreadsheet::WriteExcel->new($File->{filename}) or do {
			$logger->error("xls file creation error: $!");
			return 0;
		};
	} elsif ($File->{format_xlformat} =~ /^xlsx$/i) {
		$logger->debug("writing to xlsx format file ".$File->{filename});
		$workbook = Excel::Writer::XLSX->new($File->{filename}) or do {
			$logger->error("xlsx file creation error: $!");
			return 0;
		};
	} else {
		$logger->error("unrecognised excel format passed in \$File->{format_xlformat}:".$File->{format_xlformat}." (allowed: xls and xlsx)");
		return 0;
	}
	# Add a worksheet
	$worksheet = $workbook->add_worksheet();

	$logger->debug("fields: @columnnames");
	my @headerRow;
	for my $colname (@columnnames) {
		if (!$File->{columnskip}{$colname}) {
			push @headerRow, $colname;
		}
	}
	# write header
	unless ($File->{format_suppressHeader}) {
		for my $col (0 .. @headerRow) {
			$worksheet->write(0,$col,$headerRow[$col]);
		}
	}
	
	# write data
	$logger->trace("passed data:\n".Dumper($data)) if $logger->is_trace;
	for (my $i=0; $i<scalar(@{$data}); $i++) {
		# data row
		my $row = $data->[$i];
		my @lineRow;
		# chain all data in a row
		for my $colname (@columnnames) {
			if (!$File->{columnskip}{$colname}) {
				$logger->error("row passed in (\$process->{data}) is no ref to hash! should be \$VAR1 = {'key' => 'value', ...}:\n".Dumper($row)) if (ref($row) ne "HASH");
				my $value = $row->{$colname};
				$logger->trace("\$value for \$colname $colname: $value") if $logger->is_trace;
				if ($File->{addtlProcessingTrigger} && $File->{addtlProcessing}) {
					eval $File->{addtlProcessingTrigger} if (eval $File->{addtlProcessingTrigger});
					$logger->error("error in eval addtlProcessing: ".$File->{addtlProcessingTrigger}.":".$@) if ($@);
				}
				push @lineRow, $value;
			}
		}
		for my $col (0 .. @lineRow) {
			$worksheet->write($i+1,$col,$lineRow[$col]);
		}
		$logger->trace("row: ".@lineRow) if $logger->is_trace();
	}
	return 1;
}
1;
__END__
=head1 NAME

ETL::Wrap::File - read/parse Files from the filesystem or write to the filesystem

=head1 SYNOPSIS

 readText ($File, $process, $filenames)
 readExcel ($File, $process, $filenames)
 readXML ($File, $process, $filenames)
 writeText ($File, $process)
 writeExcel ($File, $process)

=head1 DESCRIPTION

=item readText: reads the defined text file with specified parameters into array of hashes (DB ready structure)

 $File      .. hash ref for File specific configuration
 $process   .. hash ref for process specific configuration and returned data (hashkey "data" -> above mentioned array of hashes)
 $filenames .. array of file names, if explizit (given in case of mget and unpacked zip archives).

=item readExcel: reads the defined excel file with specified parameters into array of hashes (DB ready structure)

 $File      .. hash ref for File specific configuration
 $process   .. hash ref for process specific configuration and returned data (hashkey "data" -> above mentioned array of hashes)
 $filenames .. array of file names, if explizit (given in case of mget and unpacked zip archives).

=item cell_handler: callback event handler for readExcel (xls format)

=item row_handlerXLSX: callback event handler for readExcel (xlsx format)

=item readXML: reads the defined XML file with specified parameters into array of hashes (DB ready structure)

 $File      .. hash ref for File specific configuration
 $process   .. hash ref for process specific configuration and returned data (hashkey "data" -> above mentioned array of hashes)
 $filenames .. Array von filenamen, falls explizit array of file names, if explizit (given in case of mget and unpacked zip archives).

=item writeFile: writes a text file using specified parameters from array of hashes (DB structure) 

 $File      .. hash ref for File specific configuration
 $process   .. hash ref for process specific configuration and returned data (hashkey "data" -> above mentioned array of hashes)

=item writeExcel: writes an excel file using specified parameters from array of hashes (DB structure) 

 $File      .. hash ref for File specific configuration
 $process   .. hash ref for process specific configuration and returned data (hashkey "data" -> above mentioned array of hashes)

=cut


=head1 COPYRIGHT

Copyright (c) 2023 Roland Kapl

All rights reserved.  This program is free software; you can
redistribute it and/or modify it under the same terms as Perl itself.

The full text of the license can be found in the LICENSE file included
with this module.

=cut