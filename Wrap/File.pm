package ETL::Wrap::File;

use strict; 
use Text::CSV; use Data::XLSX::Parser; use Spreadsheet::ParseExcel; use Exporter;
use Log::Log4perl qw(get_logger); use Time::localtime; use Data::Dumper; use XML::LibXML; use Scalar::Util qw(looks_like_number); use XML::LibXML::Debugging;
use Encode; use ETL::Wrap::DateUtil; 

our @ISA = qw(Exporter);
our @EXPORT = qw(readText readExcel readXML writeText);

# read text file
sub readText {
	my ($File, $process, $filenames) = @_;
	my $logger = get_logger();
	my @filenames = @{$filenames} if $filenames;
	my $redoSubDir = $process->{redoDir}."/" if $process->{redoFile};
	my $lineProcessing = $File->{LineCode};
	my $fieldProcessing = $File->{FieldCode};
	my $fieldProcessingSpec = $File->{FieldCodeSpec};
	my $addtlProcessingTrigger = $File->{addtlProcessingTrigger};
	my $addtlProcessing = $File->{addtlProcessing};
	my $firstLineProc = $File->{firstLineProc};
	for my $filename (@filenames) {
		# Format Konfiguration einlesen
		my ($poslen, $isFixLen, $skip, $sep, $autoheader); 
		my (@header, @syncheader);
		if (ref($File->{format}) eq "HASH") {
			$skip = $File->{format}{skip} if $File->{format}{skip};
			$sep = $File->{format}{sep} if $File->{format}{sep};
			$autoheader = $File->{format}{autoheader} if $File->{format}{autoheader};
			my $origsep = $sep;
			if ($sep =~ /^fix/) {
				my ($evalString) = ($sep =~ /fix=(.*?)$/);
				# position/l�ngen definition aus der sep definition holen: zB "fix=$poslen=[(0,3),(3,3)]"
				eval $evalString;
				$logger->error("eval position/length evalString: ".$evalString.$@) if ($@);
				$sep = ";";
				$isFixLen = 1;
			}
			else {
				if (!$autoheader) {
					$sep = ";" if !$sep;
				}
			}
			@header = split $sep, $File->{format}{header} if $File->{format}{header};
			@syncheader = split $sep, $File->{format}{syncheader} if $File->{format}{syncheader};
			$logger->debug("skip: $skip ,sep: [".$origsep."], header: @header \nsyncheader: @syncheader \nlineProcessing:".$File->{LineCode}."\nfieldProcessing:".$File->{FieldCode}."\nfieldProcessingSpec:".Dumper($File->{FieldCodeSpec})."\naddtlProcessingTrigger:".$File->{addtlProcessingTrigger}."\naddtlProcessing:".$File->{addtlProcessing}."\nfirstLineProc:".$firstLineProc);
		} else {
			$logger->error("keine format definitionen gefunden !!");
			return 0;
		}
		# vorhandener sync header �berschreibt immer header !
		if ($File->{format}{syncheader}) {
			$File->{headings} = \@syncheader;
		} else {
			$File->{headings} = \@header;
		}
		# Datenfile �ffnen
		open (FILE, "<".$File->{encoding}, $redoSubDir.$filename) or do { #
			if (! -e $redoSubDir.$filename) {
				$logger->error("no file $redoSubDir$filename to process...") unless ($File->{optional});
				$logger->warn("no file $redoSubDir$filename found... "); # nur f�rs protokoll (kein mail oder so)
			} else {
				$logger->error("file open error: $!")
			}
			return 0;
		};
		my $sv = Text::CSV->new ({
			binary    => 1,
			auto_diag => 1,
			sep_char  => $sep
		});
		# erzeuge lokalen kontext f�r line record separator �nderung
		{
			my $newRecSep;
			if ($File->{allowLinefeedInData}) {
				# binmode und line record separator auf CRLF setzen, damit in Werten enthaltene linefeeds beim Einlesen nicht zu k�nstlichen neuen Zeilen f�hren.
				binmode(FILE, ":raw".$File->{encoding}); # raw deshalb, weil CRLF hier sonst geschluckt werden...
				$newRecSep = "\015\012";
				$logger->debug("binmode");
			}
			# record separator (standard ist CRLF) �ndern, wenn notwendig:
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
				$skip-- if $firstLineProc; # eine zeile weniger �bergehen, wenn durch firstLineProc schon konsumiert..
				$logger->debug("skip another $skip lines..");
				# �bergehe erste $skip zeilen im datenfile (evtl. report header) wenn eine Ganzzahl oder falls $skip ein text ist, bis zum Auftreten des textes (inklusive):
				if ($skip =~ /^\d+$/) {
					for (1 .. $skip) {$_ = <FILE>};
				} else {
					while (<FILE>) {
						$_ = encode('cp1252', $_) if $File->{encoding};
						last if /$skip/;
					}
				}
			}
			# autoheader: simple csv daten ohne kopfzeilenkenntnis (annahme: erste Zeile ist header)...
			if ($autoheader) {
				$sep = "," if !$sep;
				$_ = <FILE>; chomp;
				$_ = encode('cp1252', $_) if $File->{encoding};
				@header = split $sep;
				$File->{headings} = \@header;
				$logger->debug("autoheader set, sep: [".$sep."], headings: @header");
			}

			# durch alle zeilen des Datenfiles iterieren ...
			my $lineno = 0;
			my (@line,@previousline);
LINE:
			while (<FILE>) {
				$_ = encode('cp1252', $_) if $File->{encoding};
				chomp;
				# falls $linecode auf die ganze Zeile zugreifen muss, dann mit $rawline ...
				our $rawline = $_;
				# leere Zeilen �bergehen
				next LINE if $_ eq "";
				
				# finale datenstruktur f�r die eingelesenen daten ist %line.
				# wenn das feld durch einen anderen namen aus syncheader ersetzt wird, wird das feld mit dem original namen in %templine abgelegt (f�r weitere Verwendung in $lineProcessing)
				# Der wert wird unter $line{$syncheader[$i]} abgelegt.
				# das ganze wird auch f�r die vorhergehende zeile (%previousline) und deren tempzeile (%previoustempline) gemacht.
				my (%line,%templine,%previousline,%previoustempline);
				@previousline = @line;
				if ($isFixLen) {
					@line = undef;
					for (my $i=0;$i<@header;$i++) {
						$line[$i] = substr ($_, $poslen->[$i][0],$poslen->[$i][1]-$poslen->[$i][0]);
					}
				}
				else {
					if ($File->{format}{quotedcsv}) {
						if ($sv->parse($_)) {
							@line = $sv->fields();
						} else {
							$logger->error("Zeile konnte nicht geparsed werden: ".$sv->error_diag());
						}
					} else {
						@line = split $sep;
					}
				}
				$logger->trace('raw line: '.Dumper(\@line)) if $logger->is_trace;
				$lineno++;
				next LINE if $line[0] eq "" and !$lineProcessing;
				our $skipLineAssignment = 0; # kann im FieldCode oder FieldCodeSpec gesetzt werden, um weiter unten die normale Zuweisung der ganzen Zeile zu verhindern...
				# durch alle Felder der aktuellen Zeile iterieren ...
				for (my $i=0;$i<@line;$i++) {
					# $fieldProcessing bzw.  $fieldProcessingSpec ersetzen normale Zuweisungen vollst�ndig !
					our $skipAssignment = 0; # kann im FieldCode oder FieldCodeSpec gesetzt werden, um weiter unten die normale Zuweisung zu verhindern (essenziell, wenn dies durch den FieldCode gemacht wird !)
					if ($fieldProcessing || $fieldProcessingSpec->{$syncheader[$i]} || $fieldProcessingSpec->{$header[$i]}) {
						# im Config wurde Feldverarbeitung $File{<typ>}{FieldCode} bef�llt, ersetzt Feldbearbeitung generell...
						if ($fieldProcessing) {
							$logger->trace('BEFORE: $header['.$i.']:'.$header[$i].',$line['.$i.']:'.$line[$i].",fieldProcessing:".$fieldProcessing) if $logger->is_trace;
							eval $fieldProcessing;
							$logger->error("eval fieldProcessing: ".$fieldProcessing.$@) if ($@);
							$logger->trace('AFTER: $header['.$i.']:'.$header[$i].',$line['.$i.']:'.$line[$i]."<<line: $lineno") if $logger->is_trace;
						}
						# im Config wurde Feldverarbeitung $File{<typ>}{FieldCodeSpec}{<feld>} bef�llt, ersetzt Feldbearbeitung nur f�r spezifisches Feld entweder aus syncheader oder header...
						elsif ($fieldProcessingSpec->{$syncheader[$i]}) {
							$logger->trace('BEFORE: $syncheader['.$i.']:'.$syncheader[$i].',$line['.$i.']:'.$line[$i].",fieldProcessingSpec{syncheader[i]}:".$fieldProcessingSpec->{$syncheader[$i]}) if $logger->is_trace;
							eval $fieldProcessingSpec->{$syncheader[$i]};
							$logger->error("eval fieldProcessingSpec->{syncheader[i]}: ".$fieldProcessingSpec->{$syncheader[$i]}.$@) if ($@);
							$logger->trace('AFTER: $syncheader['.$i.']:'.$syncheader[$i].',$line['.$i.']:'.$line[$i]."<<line: $lineno") if $logger->is_trace;
						}
						elsif ($fieldProcessingSpec->{$header[$i]}) {
							$logger->trace('BEFORE: $header['.$i.']:'.$header[$i].',$line['.$i.']:'.$line[$i].",fieldProcessingSpec{header[i]}:".$fieldProcessingSpec->{$header[$i]}) if $logger->is_trace;
							eval $fieldProcessingSpec->{$header[$i]};
							$logger->error("eval fieldProcessingSpec->{header[i]}: ".$fieldProcessingSpec->{$header[$i]}.$@) if ($@);
							$logger->trace('AFTER: $header['.$i.']:'.$header[$i].',$line['.$i.']:'.$line[$i]."<<line: $lineno") if $logger->is_trace;
						}
					} else { # normale zuweisungen zum entsprechenden feld (bezeichnet mit header)
						# zuerst f�hrende und nachstehende leerzeichen entfernen
						$line[$i] =~ s/^ *//;
						$line[$i] =~ s/ *$//;
						# Zahlen (und NUR Zahlen) in DBI format konvertieren ( \d+\.?\d* ) auf Basis des konfigurierten locales
						$line[$i] =~ s/\,//g if ($File->{locale} eq "english" and $line[$i] =~ /^-?\d{1,3}(\,\d{3})+(\.\d*)?$/);
						$line[$i] =~ s/\.//g if ($File->{locale} eq "german" and $line[$i] =~ /^-?\d{1,3}(\.\d{3})+(\,\d*)?$/);
						$line[$i] =~ s/\,/\./ if ($line[$i] =~ /^-?\d+\,\d+$/);
					}
					$logger->trace('verarbeitete das feld mit header:'.$header[$i].', $skipAssignment: '.$skipAssignment.', $skipLineAssignment: '.$skipLineAssignment) if $logger->is_trace;

					if (!($skipAssignment or $skipLineAssignment)) {
						# nur syncheader verarbeitung, wenn syncheader definiert, ungleich den urspr�nglichen headern und felddefinition in syncheader enthalten
						if ($File->{format}{syncheader} && ($header[$i] ne $syncheader[$i])) {
							# if $header[$i] bzw if $syncheader[$i] verhindert autovivifikation von leeren hasheintr�gen, wenn $i f�r @header oder if @syncheader �berschritten ist
							$line{$syncheader[$i]} = $line[$i] if $syncheader[$i];
							$previousline{$syncheader[$i]} = $previousline[$i] if $syncheader[$i];
							$templine{$header[$i]} = $line[$i] if $header[$i];
							$previoustempline{$header[$i]} = $previousline[$i] if $header[$i];
						} else {
							$line{$header[$i]} = $line[$i] if $header[$i];
							$previousline{$header[$i]} = $previousline[$i] if $header[$i];
						}
					}
					# wird zus�tzliche (Feld)Verarbeitung getriggert (angegeben in format => {addtlProcTrigger => "feldname", addtlProc="$addtlProcessing"})
					# im Config wurde (Feld)Zusatzverarbeitung $File{<typ>}{addtlProcessing} bef�llt...
					if (($addtlProcessingTrigger eq "*" or $header[$i] eq $addtlProcessingTrigger) && $header[$i]) { # && $header[$i], da es mitunter vorkommt, dass $header[$i] leer/undefiniert ist.
						$logger->trace('BEFORE: $header['.$i.']:'.$header[$i].',$line['.$i.']:'.$line[$i].",addtlProcessing:".$addtlProcessing) if $logger->is_trace;
						eval $addtlProcessing;
						$logger->error("eval addtlProcessing: $addtlProcessing".$@) if ($@);
						$logger->trace('AFTER: $header['.$i.']:'.$header[$i].',$line['.$i.']:'.$line[$i]."<<line: $lineno") if $logger->is_trace;
					}
				}

				# im Config wurde (zus�tzliche) Zeilenverarbeitung $File{<typ>}{LineCode} bef�llt...
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
				$logger->trace('line:\n'.Dumper(\%line)) if $logger->is_trace;
				push @{$process->{data}}, \%line if %line;
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
	$stopOnEmptyValueColumn = $File->{format}{stopOnEmptyValueColumn};
	$stoppedOnEmptyValue = 0; # reset
	my @filenames = @{$filenames} if $filenames;
	my $redoSubDir = $process->{redoDir}."/" if $process->{redoFile};
	my $lineProcessing = $File->{LineCode};
	my $fieldProcessing = $File->{FieldCode};
	my $fieldProcessingSpec = $File->{FieldCodeSpec};
	my $addtlProcessingTrigger = $File->{addtlProcessingTrigger};
	my $addtlProcessing = $File->{addtlProcessing};

	for my $filename (@filenames) {
		# reset module global variables
		$startRow = 1; 
		%dateColumn = undef;
		%headerColumn = undef;
		%dataRows = undef;
		$maxRow = 1;
		%xlheader=  undef;
		
		# read format configuration
		my (@header, @syncheader);
		if (ref($File->{format}) eq "HASH") {
			my $sep = "\t";
			$startRow += $File->{format}{skip} if $File->{format}{skip};
			$logger->error("keine header definiert") if !$File->{format}{header};
			$logger->error("keine syncheader definiert") if !$File->{format}{syncheader};
			@header = split $sep, $File->{format}{header};
			@syncheader = split $sep, $File->{format}{syncheader};
			$logger->debug("skip: ". $File->{format}{skip}.", header: @header \nsyncheader: @syncheader \nlineProcessing:".$File->{LineCode}."\nfieldProcessing:".$File->{FieldCode}."\nfieldProcessingSpec:".Dumper($File->{FieldCodeSpec})."\naddtlProcessingTrigger:".$File->{addtlProcessingTrigger}."\naddtlProcessing:".$File->{addtlProcessing});
		} else {
			$logger->error("keine format definitionen gefunden !!");
			return 0;
		}
		# sync header (finale field names for database) always defines header !
		$File->{headings} = \@syncheader;
		# check excel file existence
		if (! -e $redoSubDir.$filename) {
			$logger->error("kein excel file ($filename) zu verarbeiten...") unless ($File->{optional});
			$logger->warn("kein file $redoSubDir$filename gefunden... "); # nur f�rs protokoll (kein mail oder so)
			return 0;
		}
		# prepare date field lookup
		if ($File->{format}{dateColumns}) {
			for my $col (@{$File->{format}{dateColumns}}) {
				$dateColumn{$col} = 1;
			}
		}
		# prepare column lookups (headerColumn: needed excel column -> target field name resp. xlheader: column -> expected header content)
		my $i=0;
		for my $col (@{$File->{format}{headerColumns}}) {
			$headerColumn{$col} = $syncheader[$i];
			$xlheader{$col} = $header[$i];
			$i++;
		}

		@header = @syncheader;
		my $parser;
		if ($File->{format}{xlsx}) {
			$logger->info("open xlsx file $redoSubDir$filename ... ");
			$parser = Data::XLSX::Parser->new;
			$parser->open($redoSubDir.$filename);
			$parser->add_row_event_handler(\&row_handlerXLSX);

			if ($File->{format}{worksheet}) {
				$worksheet = $parser->workbook->sheet_id($File->{format}{worksheet});
				$logger->logdie("no worksheet found named ".$File->{format}{worksheet}.", maybe try {format}{worksheetID} (numerically ordered place)") if !$worksheet;
			} elsif ($File->{format}{worksheetID}) {
				$worksheet = $File->{format}{worksheetID};
			} else {
				$logger->logdie("neither worksheetname nor worksheetID (numerically ordered place) given");
			}
			$logger->info("starting parser for sheet name: ".$File->{format}{worksheet}.", id:".$worksheet);
			$parser->sheet_by_id($worksheet);
		} else {
			$logger->warn("worksheets can't be found by name for the old xls format, please pass numerically ordered place in {format}{worksheetID}") if ($File->{format}{worksheet});
			$worksheet = $File->{format}{worksheetID} if $File->{format}{worksheetID};
			$logger->info("starting parser for xls file $redoSubDir$filename ... ");
			$parser = Spreadsheet::ParseExcel->new(
				CellHandler => \&cell_handler,
				NotSetCell  => 1
			);
			my $workbook = $parser->parse($redoSubDir.$filename);
			if ( !defined $workbook ) {
				$logger->error("excel parsing error: ".$parser->error());
				return 0;
			}
		}

		# iterate rows
		my (@line,@previousline);
LINE:
		for my $lineno ($startRow+1 .. $maxRow) {
			@previousline = @line;
			@line = undef;
			# Zeile @line aus den zwischengespeicherten werten so vorbereiten, wie sie sonst auch aussieht
			for (my $i = 0; $i < @{$File->{format}{headerColumns}}; $i++) {
				$line[$i] = $dataRows{$lineno}{$header[$i]};
			}
			# finale datenstruktur f�r die eingelesenen daten ist %line.
			# wenn das feld durch einen anderen namen aus syncheader ersetzt wird, wird das feld mit dem original namen in %templine abgelegt (f�r weitere Verwendung in $lineProcessing)
			# Der wert wird unter $line{$syncheader} abgelegt.
			# das ganze wird auch f�r die vorhergehende zeile (%previousline) und deren tempzeile (%previoustempline) gemacht.
			my (%line,%templine,%previousline,%previoustempline);
			# durch alle Felder der aktuellen Zeile iterieren ...
			our $skipLineAssignment = 0; # kann im FieldCode oder FieldCodeSpec gesetzt werden, um weiter unten die normale Zuweisung der ganzen Zeile zu verhindern...
			for (my $i = 0; $i < @{$File->{format}{headerColumns}}; $i++) {
				# $fieldProcessing bzw.  $fieldProcessingSpec ersetzen normale Zuweisungen vollst�ndig !
				our $skipAssignment = 0; # kann im FieldCode oder FieldCodeSpec gesetzt werden, um weiter unten die normale Zuweisung zu verhindern (essenziell, wenn dies durch den FieldCode gemacht wird !)
				if ($fieldProcessing || $fieldProcessingSpec->{$syncheader[$i]} || $fieldProcessingSpec->{$header[$i]}) {
					# im Config wurde Feldverarbeitung $File{<typ>}{FieldCode} bef�llt, ersetzt Feldbearbeitung generell...
					if ($fieldProcessing) {
						$logger->trace('BEFORE: $header['.$i.']:'.$header[$i].',$line['.$i.']:'.$line[$i].",fieldProcessing:".$fieldProcessing) if $logger->is_trace;
						eval $fieldProcessing;
						$logger->error("eval fieldProcessing: ".$fieldProcessing.$@) if ($@);
						$logger->trace('AFTER: $header['.$i.']:'.$header[$i].',$line['.$i.']:'.$line[$i]."<<line: $lineno") if $logger->is_trace;
					}
					# im Config wurde Feldverarbeitung $File{<typ>}{FieldCodeSpec}{<feld>} bef�llt, ersetzt Feldbearbeitung nur f�r spezifisches Feld aus syncheader/header...
					elsif ($fieldProcessingSpec->{$header[$i]}) {
						$logger->trace('BEFORE: $header['.$i.']:'.$header[$i].',$line['.$i.']:'.$line[$i].",fieldProcessingSpec{header[i]}:".$fieldProcessingSpec->{$header[$i]}) if $logger->is_trace;
						eval $fieldProcessingSpec->{$header[$i]};
						$logger->error("eval fieldProcessingSpec->{header[i]}: ".$fieldProcessingSpec->{$header[$i]}.$@) if ($@);
						$logger->trace('AFTER: $header['.$i.']:'.$header[$i].',$line['.$i.']:'.$line[$i]."<<line: $lineno") if $logger->is_trace;
					}
				} else { # normale zuweisungen zum entsprechenden feld (bezeichnet mit header)
					# zuerst f�hrende und nachstehende leerzeichen entfernen
					$line[$i] =~ s/^ *//;
					$line[$i] =~ s/ *$//;
					# Zahlen (und NUR Zahlen) in DBI format konvertieren ( \d+\.?\d* ) auf Basis des konfigurierten locales
					$line[$i] =~ s/\,//g if ($File->{locale} eq "english" and $line[$i] =~ /^-?\d{1,3}(\,\d{3})+(\.\d*)?$/);
					$line[$i] =~ s/\.//g if ($File->{locale} eq "german" and $line[$i] =~ /^-?\d{1,3}(\.\d{3})+(\,\d*)?$/);
					$line[$i] =~ s/\,/\./ if ($line[$i] =~ /^-?\d+\,\d+$/);
				}
				$logger->trace('verarbeitete das feld mit header:'.$header[$i].', $skipAssignment: '.$skipAssignment.', $skipLineAssignment: '.$skipLineAssignment) if $logger->is_trace;

				if (!($skipAssignment or $skipLineAssignment)) {
					$line{$header[$i]} = $line[$i];
					$previousline{$header[$i]} = $previousline[$i];
				}
				# wird zus�tzliche (Feld)Verarbeitung getriggert (angegeben in format => {addtlProcTrigger => "feldname", addtlProc="$addtlProcessing"})
				# im Config wurde (Feld)Zusatzverarbeitung $File{<typ>}{addtlProcessing} bef�llt...
				if (($addtlProcessingTrigger eq "*" or $header[$i] eq $addtlProcessingTrigger) && $header[$i]) { # && $header[$i], da es mitunter vorkommt, dass $header[$i] leer/undefiniert ist.
					$logger->trace('BEFORE: $header['.$i.']:'.$header[$i].',$line['.$i.']:'.$line[$i].",addtlProcessing:".$addtlProcessing) if $logger->is_trace;
					eval $addtlProcessing;
					$logger->error("eval addtlProcessing: $addtlProcessing".$@) if ($@);
					$logger->trace('AFTER: $header['.$i.']:'.$header[$i].',$line['.$i.']:'.$line[$i]."<<line: $lineno") if $logger->is_trace;
				}
			}
			# im Config wurde (zus�tzliche) Zeilenverarbeitung $File{<typ>}{LineCode} bef�llt...
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
		close FILE;
		if (!$process->{data} and !$File->{emptyOK}) {
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
	my $lineProcessing = $File->{LineCode};
	my $fieldProcessing = $File->{FieldCode};
	my $fieldProcessingSpec = $File->{FieldCodeSpec};
	my $addtlProcessingTrigger = $File->{addtlProcessingTrigger};
	my $addtlProcessing = $File->{addtlProcessing};

	for my $filename (@filenames) {
		# reset module global variables
		%headerColumn = undef;
		%dataRows = undef;
		# read format configuration
		my (@header, @syncheader);
		if (ref($File->{format}) eq "HASH") {
			my $sep = "\t";
			$logger->error("no header defined") if !$File->{format}{header};
			@header = split $sep, $File->{format}{header};
			$logger->debug("header: @header \nlineProcessing:".$File->{LineCode}."\nfieldProcessing:".$File->{FieldCode}."\nfieldProcessingSpec:".Dumper($File->{FieldCodeSpec})."\naddtlProcessingTrigger:".$File->{addtlProcessingTrigger}."\naddtlProcessing:".$File->{addtlProcessing});
		} else {
			$logger->error("no format definitions found");
			return 0;
		}
		$File->{headings} = \@header;

		# check file
		if (! -e $redoSubDir.$filename) {
			$logger->error("no XML file ($filename) to process") unless ($File->{optional});
			$logger->warn("file $redoSubDir$filename not found");
			return 0;
		}

		my $xmldata = XML::LibXML->load_xml(location => $redoSubDir.$filename, no_blanks => 1);
		my $xpc = XML::LibXML::XPathContext->new($xmldata);
		if (ref($File->{format}{namespaces}) eq 'HASH') {
			$xpc->registerNs($_, $File->{format}{namespaces}{$_}) for keys (%{$File->{format}{namespaces}});
		}
		$logger->error("no xpathRecordLevel passed in format") unless ($File->{format}{xpathRecordLevel});
		$logger->error("no fieldXpath hash passed in format") unless ($File->{format}{fieldXpath} && ref($File->{format}{fieldXpath}) eq 'HASH');
		my @records = $xpc->findnodes($File->{format}{xpathRecordLevel});
		$logger->warn("no records found") if @records == 0;
		foreach my $record (@records) {
			my %line;
			if (ref($record) eq "XML::LibXML::Element") {
				for my $field (keys (%{$File->{format}{fieldXpath}})) {
					if ($File->{format}{fieldXpath}{$field} =~ /^\//) {
						# absolute paths -> leave context node and find in the root doc. 
						$line{$field} = $xpc->findvalue($File->{format}{fieldXpath}{$field});
					} else {
						# relative paths -> context node is current record node 
						$line{$field} = $xpc->findvalue($File->{format}{fieldXpath}{$field}, $record);
					}
				}
			}
			$logger->trace("line:\n".Dumper(\%line)) if $logger->is_trace;
			push @{$process->{data}}, \%line if %line;
		}
		if (!$process->{data} and !$File->{emptyOK}) {
			$logger->error("empty file: $filename, no data returned");
			return 0;
		}
	}
	return 1;
}

# write text file
sub writeText {
	my ($File, $process) = @_;
	my $logger = get_logger();
	
	my $filename = $File->{filename};
	my $data = $process->{data};
	my $beforeHeader = $File->{beforeHeader};
	
	my @columnnames; my @paddings;
	if (ref($File->{columns}) eq 'HASH') {
		@columnnames = map {$File->{columns}{$_}} sort keys %{$File->{columns}};
	} else {
		$logger->error("no field information given (columns => ref to hash)");
		return 0;
	}
	if (ref($File->{padding}) eq 'HASH') {
		@paddings = map {$File->{padding}{$_}} sort keys %{$File->{padding}};
	} else {
		if ($File->{format} eq "fix") {
			$logger->error("no padding information given for fixed length format (padding => ref to hash)");
			return 0;
		}
	}
	$logger->debug("fields: @columnnames");
	$logger->debug("paddings: @paddings");
	my $headerRow;
	my $col = 0; # parallel durch @paddings iterieren.
	my $firstcol = 1;
	for my $colname (@columnnames) {
		if (!$File->{columnskip}{$colname}) {
			# erste Spalte hat kein Trennzeichen davor. Wenn spezielles Trennzeichen f�r Kopfzeile, dann dieses verwenden, sonst das allgemeine.
			$headerRow = $headerRow.($firstcol ? "" : ($File->{sepHead} ? $File->{sepHead} : $File->{sep})).$colname if ($File->{format} eq "sep");
			$headerRow = $headerRow.sprintf("%-*s%s", $paddings[$col],$colname) if ($File->{format} eq "fix");
			$firstcol = 0;
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
	print FHOUT $beforeHeader if $beforeHeader;
	print FHOUT $headerRow."\n" unless $File->{suppressHeader};
	
	# write data
	$logger->trace("passed data:\n".Dumper($data)) if $logger->is_trace;
	for (my $i=0; $i<scalar(@{$data}); $i++) {
		# data row
		my $row = $data->[$i];
		my $lineRow;
		# alle spalten in eine Zeile zusammenh�ngen
		my $col = 0; $firstcol = 1;
		for my $colname (@columnnames) {
			if (!$File->{columnskip}{$colname}) {
				$logger->error("row passed in (\$process->{data}) is no ref to hash! should be \$VAR1 = {'key' => 'value', ...}:\n".Dumper($row)) if (ref($row) ne "HASH");
				my $value = $row->{$colname};
				$logger->trace("value for $colname: $value") if $logger->is_trace;
				if ($File->{additionalColTrigger} && $File->{additionalColAction}) {
					eval $File->{additionalColAction} if (eval $File->{additionalColTrigger});
				}
				# letzte spalte ($columnnames[@columnnames-1], (arrays in skalarem kontext ergibt die l�nge des array)) sollte kein Trennzeichen zum schluss haben.
				$lineRow = $lineRow.($firstcol ? "" : $File->{sep}).sprintf("%s", $value) if ($File->{format} eq "sep");
				# zus�tzliches padding f�r fixl�ngen format
				$lineRow = $lineRow.sprintf("%-*s%s", $paddings[$col],$value) if ($File->{format} eq "fix");
				$firstcol = 0;
			}
			$col++;
		}
		print FHOUT $lineRow."\n";
		$logger->trace("row: ".$lineRow) if $logger->is_trace();
	}
	close FHOUT;
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

=cut


=head1 COPYRIGHT

Copyright (c) 2022 Roland Kapl

All rights reserved.  This program is free software; you can
redistribute it and/or modify it under the same terms as Perl itself.

The full text of the license can be found in the LICENSE file included
with this module.

=cut