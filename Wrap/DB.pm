package ETL::Wrap::DB;

use strict; 
use DBI qw(:sql_types); use DBD::ODBC; use Data::Dumper; use Log::Log4perl qw(get_logger); use Exporter;

our @ISA = qw(Exporter);
our @EXPORT = qw(newDBH beginWork commit rollback readFromDB readFromDBHash doInDB storeInDB deleteFromDB updateInDB);

my $dbh;
my $DSN;

sub newDBH {
	my ($DB,$execute) = @_;
	my $logger = get_logger();
	my ($DSNeval, $newDSN);
	$DSNeval = ($DB->{isTrusted} ? $DB->{DSNTrusted} : $DB->{DSNUntrusted});
	$newDSN = eval qq{"$DSNeval"};
	$logger->error("error parsing \$DB->{".($DB->{isTrusted} ? "DSNTrusted" : "DSNUntrusted")."} (couldn't interpolate all values):".$DSNeval) if !$newDSN;
	if ($DSN ne $newDSN or !defined($dbh)) {
		$DSN = $newDSN;
		$logger->debug("DSN: $DSN");
		$dbh->disconnect() if defined($dbh);
		$dbh = DBI->connect("dbi:ODBC:$DSN",undef,undef,{PrintError => 0,RaiseError => 0}) or do {
			$logger->error("DB connection error:".$DBI::errstr.",DSN:".$DSN);
			undef $dbh;
			return 0;
		};
		$DB->{longreadlen} = 1024 if !$DB->{longreadlen};
		$dbh->{LongReadLen} = $DB->{longreadlen};
	} else {
		$logger->debug("DB connection already open, using $DSN");
		return 1;
	}
}

sub beginWork {
	my $logger = get_logger();
	if (!defined($dbh)) {
		$logger->error("no valid dbh connection!");
		return 0;
	}
	$dbh->begin_work or do {$logger->error($DBI::errstr ); return 0};
	return 1;
}

sub commit {
	my $logger = get_logger();
	if (!defined($dbh)) {
		$logger->error("no valid dbh connection!");
		return 0;
	}
	$dbh->commit or do {$logger->error($DBI::errstr); return 0};
	return 1;
}

sub rollback {
	my $logger = get_logger();
	if (!defined($dbh)) {
		$logger->error("no valid dbh connection!");
		return 0;
	}
	$dbh->rollback or do {$logger->error($DBI::errstr); return 0};
	return 1;
}

# read data into array returned in $data
sub readFromDB {
	my ($DB, $data) = @_;
	my $statement = $DB->{query};
	my $logger = get_logger();
	if (!defined($dbh)) {
		$logger->error("no valid dbh connection!");
		return 0;
	}
	if (!$statement) {
		$logger->error("no statement (query) given !");
		return 0;
	}
	$logger->debug("statement: ".$statement);
	my $data;
	$dbh->{RaiseError} = 1; # to enable following try/catch (eval)
	eval {
		my $sth = $dbh->prepare($statement);
		$sth->execute;
		@{$DB->{columnnames}} = @{$sth->{NAME}}; # take field names from the statement handle of query
		$data = $sth->fetchall_arrayref({});
	};
	if ($@) {
		$logger->error($@.",DB error: ".$DBI::errstr." executed statement: ".$statement);
		$dbh->{RaiseError} = 0;
		return 0;
	}
	$dbh->{RaiseError} = 0;
	return 1;
}

# read data into hash using column $DB->{keyfield} as the unique key for the hash (used for lookups), returned in $data
sub readFromDBHash {
	my ($DB, $data) = @_;
	my $statement = $DB->{query};
	my $logger = get_logger();
	if (!defined($dbh)) {
		$logger->error("no valid dbh connection!");
		return 0;
	}
	if (!$statement) {
		$logger->error("no statement (query) given !");
		return 0;
	}

	$logger->debug("statement: ".$statement);
	my $data;
	$dbh->{RaiseError} = 1; # to enable following try/catch (eval)
	eval {
		my $sth = $dbh->prepare($statement);
		$sth->execute;
		$data = $sth->fetchall_hashref($DB->{keyfield});
	};
	if ($@) {
		$logger->error($@.",DB Fehler: ".$DBI::errstr." ausgeführtes statement: ".$statement);
		$dbh->{RaiseError} = 0;
		return 0;
	}
	$dbh->{RaiseError} = 0;
	return 1;
}

# do statement $doString in database using optional parameters passed in array ref $parameters, optionally passing back values in $data
sub doInDB {
	my ($doString, $data, $parameters) = @_;
	my @parameters = @$parameters;
	
	my $logger = get_logger();
	if (!defined($dbh)) {
		$logger->error("no valid dbh connection!");
		return 0;
	}
	if (!$doString) {
		$logger->error("no sql statement given !");
		return 0;
	}
	$logger->debug("do in DB: $doString, parameters: @parameters");
	my $sth = $dbh->prepare($doString);
	if (@parameters == 0) {
		$sth->execute();
	} else {
		$sth->execute(@parameters);
	}
	if ($sth->err) {
		$logger->error($@.",DB error: ".$sth->errstr." statement: ".$doString);
		return 0;
	}
	# return record set of statement if possible/required
	do {
		push @$data, $sth->fetchall_arrayref({}) if ref($data) eq "ARRAY";
	} while ($sth->{odbc_more_results});
	return 1;
}

sub storeInDB {
	my ($DB, $data) = @_;
	my $tableName = $DB->{tablename};
	my $addID = $DB->{addID};
	my $upsert= $DB->{upsert};
	my $primkey = $DB->{primkey};
	my $ignoreDuplicateErrs = $DB->{ignoreDuplicateErrs};
	my $deleteBeforeInsertSelector = $DB->{deleteBeforeInsertSelector};
	my $incrementalStore = $DB->{incrementalStore};
	my $doUpdateBeforeInsert = $DB->{doUpdateBeforeInsert};
	my $debugKeyIndicator = $DB->{debugKeyIndicator};
	my $logger = get_logger();

	my $DBerrHappened=0;
	my @keycolumns = split "AND", $primkey;
	map { s/=//; s/\?//; s/ //g;} @keycolumns;
	$logger->debug("tableName:$tableName,addID:$addID,upsert:$upsert,primkey:$primkey,ignoreDuplicateErrs:$ignoreDuplicateErrs,deleteBeforeInsertSelector:$deleteBeforeInsertSelector,incrementalStore:$incrementalStore,doUpdateBeforeInsert:$doUpdateBeforeInsert,debugKeyIndicator:$debugKeyIndicator");
	if (!defined($dbh)) {
		$logger->error("no valid dbh connection");
		return 0;
	}
	my $schemaName = $DB->{schemaName};
	if ($tableName =~ /\./) {
		$logger->debug("getting schema from tablename (contains dot): $tableName");
		($schemaName, $tableName) = ($tableName =~ /(.*)\.(.*)/);
	}
	my $colh = $dbh->column_info ('', $schemaName, $tableName, "%");
	my $coldefs = $colh->fetchall_hashref ("COLUMN_NAME");
	if (scalar(keys %{$coldefs}) == 0) {
		$logger->error("no field definitions found for schema $schemaName, table $tableName");
		return 0;
	}
	$logger->trace("coldefs:\n".Dumper($coldefs)) if $logger->is_trace;
	my $IDName;
	if ($addID) {
		if (ref($addID) ne 'HASH') {
			$logger->error("no valid addID given (needs to be ref to hash, key=fieldname, value=ID)!");
			return 0;
		}
		$IDName = (keys %{$addID})[0];
	}
	my $i=0; my @columns;
	$logger->trace("type info:\n".Dumper($dbh->type_info("SQL_ALL_TYPES"))) if $logger->is_trace; # all availably data type informations of DBD:ODBC driver
	for (keys %{$coldefs}) {
		if ($coldefs->{$_}{"COLUMN_DEF"} =~ /identity/ || $coldefs->{$_}{"TYPE_NAME"} =~ /identity/) { # for identity (auto incrementing) fields no filling needed
			$logger->trace("TYPE_NAME for identity field ".$_.':'.$coldefs->{$_}{"TYPE_NAME"}) if $logger->is_trace;
		} else {
			$columns[$i]= $_;
			$logger->trace("TYPE_NAME for normal field ".$columns[$i].':'.$coldefs->{$columns[$i]}{"TYPE_NAME"}) if $logger->is_trace;
			$i++;
		}
	}
	# is the given field (header) in table? warn, if not
	for my $dataheader (keys %{$data->[0]}) {
		$logger->warn("field '".$dataheader."' not contained in table $schemaName.$tableName") if !$coldefs->{$dataheader} && !$DB->{dontWarnOnNotExistingFields};
	}
	$dbh->{PrintError} = 0; $dbh->{RaiseError} = 0;
	if (scalar(@{$data}) > 0) {
		$logger->trace("passed data:\n".Dumper($data)) if $logger->is_trace;
		my %beforeInsert; # hash flag for update before insert (first row where data appears)
		my $keyValueForDeleteBeforeInsert;
		# row-wise processing data ($i)
		for (my $i=0; $i<scalar(@{$data}); $i++) {
			my @dataArray; # row data that is prepared for insert/update, formatted by the type as defined in DB
			my $inscols; my $inscolVals; my $updcols; my $updselector = $primkey; # for building insert and update statements
			my $tgtCol=0; # separate target column iterator for dataArray to allow for skipping columns with $incrementalStore !
			$debugKeyIndicator = $primkey if !$debugKeyIndicator; # $debugKeyIndicator is primary if not given
			my $debugKey = $debugKeyIndicator if $debugKeyIndicator; # $debugKey is the actual value of the debug key
			$debugKey = "neither primkey nor debugKeyIndicator given, no information possible here" if !$debugKey;
			my ($errorIndicator,$severity); # collect error messages

			# iterate table fields, check data by type and format accordingly
			for (my $dbCol=0; $dbCol < scalar(@{columns}); $dbCol++) {
				$dataArray[$tgtCol] = $data->[$i]{$columns[$dbCol]}; # zuerst mit den rohdaten befüllen
				$dataArray[$tgtCol] = $addID->{$IDName} if ($addID and $columns[$dbCol] eq $IDName); # if passed: add constant ID-field to all data rows
				my $datatype = $coldefs->{$columns[$dbCol]}{"TYPE_NAME"}; # type from DB dictionary
				my $datasize = $coldefs->{$columns[$dbCol]}{"COLUMN_SIZE"}; # size from DB dictionary
				# numerical types:
				if ($datatype =~ /^numeric/ or $datatype =~ /^float/ or $datatype =~ /real/ or $datatype =~ /^smallmoney/ or $datatype =~ /^money/ or $datatype =~ /^decimal/ or $datatype =~ /^tinyint/ or $datatype =~ /^smallint/ or $datatype =~ /^int/  or $datatype =~ /^bigint/) {
					# in case we have trailing 0s after comma, get rid of them for integer fields
					$dataArray[$tgtCol] =~ s/\.0+$// if $dataArray[$tgtCol] =~ /\d+\.0+$/;
					# in case of postfix minus (SAP Convention), revert that to prefix ...
					$dataArray[$tgtCol] =~ s/([\d\.]*)-/-$1/ if $dataArray[$tgtCol] =~ /[\d\.]*-$/;
					# in case of numeric data, prevent any nonnumeric input...
					$dataArray[$tgtCol] =~ s/%$// if $dataArray[$tgtCol] =~ /[\d\.]*%$/; # get rid of percent sign
					# ignore everything that doesn't look like a numeric (also regarding scientific notation (E...)). 
					# decimal place before comma is optional, at least one decimal digit is needed (but also integers are possible)
					$dataArray[$tgtCol] = undef if !($dataArray[$tgtCol] =~ /^-*\d*\.?\d+E*[-+]*\d*$/);
					$dataArray[$tgtCol] = undef if $dataArray[$tgtCol] =~ /^N\/A$/;
					# smallest possible double for sql server = 1.79E-308
					$dataArray[$tgtCol] = 0 if abs($dataArray[$tgtCol]) <= 1.79E-308 and abs($dataArray[$tgtCol]) > 0;
				# boolean, convert text to 0 and 1.
				} elsif ($datatype =~ /^bit/) {
					$dataArray[$tgtCol] =~ s/WAHR/1/i;
					$dataArray[$tgtCol] =~ s/FALSCH/0/i;
					$dataArray[$tgtCol] =~ s/TRUE/1/i;
					$dataArray[$tgtCol] =~ s/FALSE/0/i;
				# date/time
				} elsif ($datatype =~ /^date/ or $datatype =~ /^time/) {
					# prevent non date/time
					$dataArray[$tgtCol] = undef if !($dataArray[$tgtCol] =~ /^(\d{2})[.\/]*(\d{2})[.\/]*(\d{2,4})/ or $dataArray[$tgtCol] =~ /^(\d{4})-(\d{2})-(\d{2})/ or $dataArray[$tgtCol] =~ /^(\d{2}):(\d{2}):(\d{2})/);
					# convert DD./MM./YYYY hh:mm:ss to ODBC format YYYY-MM-DD hh:mm:ss
					$dataArray[$tgtCol] =~ s/^(\d{2})[.\/](\d{2})[.\/](\d{4}) (\d{2}):(\d{2}):(\d{2})/$3-$2-$1 $4:$5:$6/ if ($dataArray[$tgtCol] =~ /^(\d{2})[.\/](\d{2})[.\/](\d{4}) (\d{2}):(\d{2}):(\d{2})/);
					# convert YYYY-MM-DD hh:mm:ss.msec to ODBC format YYYY-MM-DD hh:mm:ss
					$dataArray[$tgtCol] =~ s/^(\d{2})[.\/](\d{2})[.\/](\d{4}) (\d{2}):(\d{2}):(\d{2})/$1-$2-$3 $4:$5:$6/ if ($dataArray[$tgtCol] =~ /^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})\.\d{3}/);
					$dataArray[$tgtCol] =~ s/^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(60)/$1-$2-$3 $4:$5:59/ if ($dataArray[$tgtCol] =~ /^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(60)/); # sap doesn't pass correct time
					$dataArray[$tgtCol] =~ s/^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})/$1-$2-$3 $4:$5:$6/ if ($dataArray[$tgtCol] =~ /^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})/); # normal sap datetime
					$dataArray[$tgtCol] =~ s/^(\d{2})[.\/](\d{2})[.\/](\d{4})/$3-$2-$1 00:00:00/ if ($dataArray[$tgtCol] =~ /^(\d{2})[.\/](\d{2})[.\/](\d{4})/); # convert date values to ODBC canonical format YYYY-MM-DD 00:00:00 if no time part given
					if ($dataArray[$tgtCol] =~ /^\d{8}/) {
						$dataArray[$tgtCol] =~ s/^(\d{4})(\d{2})(\d{2})/$1-$2-$3 00:00:00/; # sap dates are yyyymmdd...
					}
					$dataArray[$tgtCol] =~ s/^(\d{4})\.(\d{2})\.(\d{2})/$1-$2-$3 00:00:00/ if ($dataArray[$tgtCol] =~ /^(\d{4})\.(\d{2})\.(\d{2})/); # date value yyyy.mm.dd
					# for date values with short year (2 digit)
					if ($dataArray[$tgtCol] =~ /^(\d{2})[.\/](\d{2})[.\/](\d{2})/) {
						my $prefix = "20";
						$prefix = "19" if $3 > $DB->{cutoffYr2000};
						$errorIndicator = "Konvertiere 2-Ziffern jahr:".$3." in 4-Ziffern jahr mit Beginin $prefix ; Cutoff = ".$DB->{cutoffYr2000};
						$severity = 0 if !$severity;
						$dataArray[$tgtCol] =~ s/^(\d{2})[.\/](\d{2})[.\/](\d{2})/$prefix$3-$2-$1 00:00:00/;
					}
					$dataArray[$tgtCol] = undef if ($dataArray[$tgtCol] =~ /^00:00:00$/);
					$dataArray[$tgtCol] = undef if ($dataArray[$tgtCol] =~ /^0000-00-00 00:00:00$/);
					$dataArray[$tgtCol] =~ s/^(\d{2}):(\d{2}):(\d{2})/1900-01-01 $1:$2:$3/ if ($dataArray[$tgtCol] =~ /^(\d{2}):(\d{2}):(\d{2})/);
					$dataArray[$tgtCol] =~ s/^(\d{2})(\d{2})(\d{2})/1900-01-01 $1:$2:$3/ if ($dataArray[$tgtCol] =~ /^(\d{6})/);
					$dataArray[$tgtCol] =~ s/^(\d{2}):(\d{2})$/1900-01-01 $1:$2:00/ if ($dataArray[$tgtCol] =~ /^(\d{2}):(\d{2})$/);
					# consistency checks
					if ($dataArray[$tgtCol] =~ /^0/) {
						$errorIndicator .= "| wrong year value (starts with 0): ".$dataArray[$tgtCol].", field: ".$columns[$dbCol];
						$dataArray[$tgtCol] = undef;
						$severity = 1 if !$severity;
					}
					if ($dataArray[$tgtCol] && $dataArray[$tgtCol] !~ /^\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}$/ && $dataArray[$tgtCol] !~ /^\d{4}\-\d{2}\-\d{2}$/) {
						$errorIndicator .= "| korrektes datumsformat (\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2} oder \d{4}\-\d{2}\-\d{2}) konnte für: ".$dataArray[$tgtCol].", Feld: ".$columns[$dbCol]." nicht erzeugt werden!";
						$dataArray[$tgtCol] = undef;
						$severity = 1 if !$severity;
					}
					$dataArray[$tgtCol] =~ s/(.*)/'$1'/ if $dataArray[$tgtCol]; # quote date/time values
				# everything else interpreted as string
				} else {
					if (length($dataArray[$tgtCol]) > $datasize) {
						$errorIndicator .= "| Inhalt '".$dataArray[$tgtCol]."' zu lang für Feld: ".$columns[$dbCol]." (Typ $datatype) in Zeile $i (0-basiert), Länge: ".length($dataArray[$tgtCol]).", definierte Feldgröße: ".$datasize;
						$severity = 2;
					}
					$dataArray[$tgtCol] =~ s/'/''/g; # quote quotes
					$dataArray[$tgtCol] =~ s/\n//g; # remove newlines if existent (File{allowLinefeedInData}=1)
					$dataArray[$tgtCol] =~ s/(.*)/'$1'/ if $dataArray[$tgtCol]; # quote strings
				}

				# build insert/update statement
				my $colName = $columns[$dbCol]; 
				my $colVal = $dataArray[$tgtCol];
				# skip field building for incrementalStore and no content was found
				unless ($incrementalStore && !defined($data->[$i]{$columns[$dbCol]})) {
					# explicit NULL for empty values
					$colVal = "NULL" if !defined($dataArray[$tgtCol]) or $dataArray[$tgtCol] eq "";
					# fill primary key values with data from current row for update (in case insert fails)
					$updselector =~ s/$colName\s*=\s*\?/\[$colName\] = $colVal/ if ($updselector =~ /^$colName\s*=\s*\?/ || $updselector =~ /AND $colName\s*=\s*\?/i);
					# delete before select statement requires specific selector to only delete once for occurred data (first appearance of colVal)
					$deleteBeforeInsertSelector =~ s/$colName\s*=\s*\?/\[$colName\] = $colVal/ if ($deleteBeforeInsertSelector =~ /^$colName\s*=\s*\?/ || $deleteBeforeInsertSelector =~ /AND $colName\s*=\s*\?/);
					$updcols.="[".$colName."] = ".$colVal.",";
					$inscols.="[".$colName."],";
					$inscolVals.=$colVal.",";
					$tgtCol++;
				}
				$debugKey =~ s/$colName\s*=\s*\?/$colName = $colVal/ if ($debugKey =~ /$colName\s*=\s*\?/); # fill debug field display (for error messages)
			}
			# log collected errors during building of statement
			$logger->error($errorIndicator.", at [".$debugKey."]") if $errorIndicator and $severity>=1;
			$logger->warn($errorIndicator.", at [".$debugKey."]") if $errorIndicator and $severity==0;
			return 0 if $errorIndicator and $severity == 2; # leave for impossible update/insert
			# delete relevant data before insert. only done once for first row that fulfills $deleteBeforeInsertSelector
			if ($deleteBeforeInsertSelector && !$beforeInsert{$deleteBeforeInsertSelector}) {
				$logger->info("deleting data from $schemaName.$tableName, criteria: $deleteBeforeInsertSelector");
				my $dostring = "delete from $schemaName.$tableName WHERE $deleteBeforeInsertSelector";
				$dbh->do($dostring) or do {
					$logger->error($DBI::errstr." bei $dostring ".$debugKeyIndicator);
					return 0;
				};
				# mark deleteBeforeInsertSelector as executed for these data
				$beforeInsert{$deleteBeforeInsertSelector} = 1;
				$logger->info("enter data into $schemaName.$tableName");
			}
			if ($logger->is_trace) {
				$logger->trace("data to be inserted:");
				$logger->trace($inscols);
				$logger->trace($inscolVals);
			}
			substr($inscols, -1) = ""; substr($inscolVals, -1) = ""; substr($updcols, -1) = ""; # remove last comma
			# only update before insert, if no deleteBeforeInsertSelector given (may not have a primkey then)
			if ($doUpdateBeforeInsert && !$deleteBeforeInsertSelector) {
				# insert data:  first try UPDATE
				my $dostring = "UPDATE $schemaName.$tableName SET $updcols WHERE $updselector";
				my $affectedRows = $dbh->do($dostring);
				if ($affectedRows == 0 or $DBI::errstr) {
					if ($affectedRows == 0 and $upsert) {
						my $dostring = "INSERT INTO $schemaName.$tableName ($inscols) VALUES (".$inscolVals.")";# ($placeholders)";
						$logger->trace("update failed (not yet existing), insert data instead: ".$dostring) if $logger->is_trace;
						# then try INSERT
						$dbh->do($dostring) or do {
							my $errRec;
							for (my $j=0; $j < scalar(@{columns}); $j++) {
								my $datatype = $coldefs->{$columns[$j]}{"TYPE_NAME"}."(".$coldefs->{$columns[$j]}{"COLUMN_SIZE"}.")";
								$errRec.= $columns[$j]."[".$datatype."]:".$dataArray[$j].", ";
							}
							$logger->error($DBI::errstr." when inserting data: $errRec, executed statement: $dostring");
							$DBerrHappened = 1;
						}
					# error message not needed to suppress for update errors without upsert flag
					} else {
						my $errRec;
						for (my $j=0; $j < scalar(@{columns}); $j++) {
							my $datatype = $coldefs->{$columns[$j]}{"TYPE_NAME"}."(".$coldefs->{$columns[$j]}{"COLUMN_SIZE"}.")";
							$errRec.= $columns[$j]."[".$datatype."]:".$dataArray[$j].", ";
						}
						$logger->error($DBI::errstr." when inserting data: $errRec, executed statement: $dostring");
						$DBerrHappened = 1;
					}
				};
			} else {
				# insert data:  first try INSERT
				my $dostring = "INSERT INTO $schemaName.$tableName ($inscols) VALUES (".$inscolVals.")";# ($placeholders)";
				$dbh->do($dostring) or do {
					$logger->trace("DBI Error:".$DBI::errstr) if $logger->is_trace;
					if ($DBI::errstr =~ /cannot insert duplicate key/i and $upsert) {
						my $dostring = "UPDATE $schemaName.$tableName SET $updcols WHERE $updselector";
						$logger->trace("insert failed (already existing), update date instead: ".$dostring) if $logger->is_trace;
						# insert data:  then try UPDATE
						$dbh->do($dostring) or do {
							my $errRec;
							for (my $j=0; $j < scalar(@{columns}); $j++) {
								my $datatype = $coldefs->{$columns[$j]}{"TYPE_NAME"}."(".$coldefs->{$columns[$j]}{"COLUMN_SIZE"}.")";
								$errRec.= $columns[$j]."[".$datatype."]:".$dataArray[$j].", ";
							}
							$logger->error($DBI::errstr." when updating data: $errRec, executed statement: $dostring");
							$DBerrHappened = 1;
						}
					# error message if needed
					} elsif (($DBI::errstr =~ /cannot insert duplicate key/i and !$ignoreDuplicateErrs) or $DBI::errstr !~ /cannot insert duplicate key/i) {
						my $errRec;
						for (my $j=0; $j < scalar(@{columns}); $j++) {
							my $datatype = $coldefs->{$columns[$j]}{"TYPE_NAME"}."(".$coldefs->{$columns[$j]}{"COLUMN_SIZE"}.")";
							$errRec.= $columns[$j]."[".$datatype."]:".$dataArray[$j].", ";
						}
						$logger->error($DBI::errstr." when updating data: $errRec, executed statement: $dostring");
						$DBerrHappened = 1;
					}
				};
			}
		}
	} else {
		$logger->error("no data to store");
		return 0;
	}
	return !$DBerrHappened;
}

sub deleteFromDB {
	my ($DB,$data) = @_;
	my $tableName = $DB->{tablename};
	my $keycol = $DB->{keyfield};
	my $logger = get_logger();
	if (!defined($dbh)) {
		$logger->error("no valid dbh connection");
		return 0;
	}
	if ((@{$data}) > 0) {
		# prepare statement
		my $whereclause = $keycol;
		$whereclause = "[$keycol] = ?" if $keycol !~ /\?/; # if $keycol only contains field names (no ?), then build clause here
		my $sth = $dbh->prepare("DELETE FROM ".$tableName." WHERE $whereclause");
		# execute with data
		for my $primkey (@{$data}) {
			$logger->trace("deleting data for $primkey") if $logger->is_trace;
			$sth->execute(($primkey)) or do {
				$logger->error("couldn't delete data for $primkey:".$DBI::errstr );
				return 0;
			}
		}
	} else {
		$logger->error("no data to update");
		return 0;
	}
	return 1;
}

sub updateInDB {
	my ($DB,$data) = @_;
	my $tableName = $DB->{tablename};
	my $keycol = $DB->{keyfield};
	my $logger = get_logger();
	if (!defined($dbh)) {
		$logger->error("no valid dbh connection");
		return 0;
	}
	my $firstrecordID = (keys %{$data})[0];
	$logger->trace("passed data:\n".Dumper($data)) if $logger->is_trace;
	my @columns = sort keys %{$data->{$firstrecordID}}; # sort to ensure deterministic ordering of fieldnames !
	my $schemaName = "dbo";
	if ($tableName =~ /\./) {
		$logger->debug("getting schema from tablename (contains dot): $tableName");
		($schemaName, $tableName) = ($tableName =~ /(.*)\.(.*)/);
	}
	my $colh = $dbh->column_info ('', $schemaName, $tableName, "%");
	my $coldefs = $colh->fetchall_hashref ("COLUMN_NAME");
	
	if ((keys %{$data}) > 0) {
		# prepare statement
		my %keycolumns;
		my @keycolumns = split "AND", $keycol;
		map { s/=//; s/\?//; s/ //g; $keycolumns{$_}=1} @keycolumns;
		my $whereclause = $keycol;
		$whereclause = "[$keycol] = ?" if $keycol !~ /\?/; # if $keycol contains only column, build clause here.
		my @dataCols;
		my $cols; 
		# build columns for set statement and start ordered columns with non key fields (key fields are appended to this below in the order of the WHERE clause)
		map {if (!$keycolumns{$_}) {$cols.="[$_] = ?,"; push @dataCols, $_}} @columns;
		substr($cols, -1)  = ""; # delete last ","
		my $sth = $dbh->prepare("UPDATE ".$tableName." SET $cols WHERE $whereclause");
		# execute with data
		for my $primkey (keys %{$data}) {
			my @dataArray;
			for (my $i=0; $i <= $#columns; $i++) {
				$dataArray[$i] = $data->{$primkey}{$dataCols[$i]};
			}
			$logger->trace("Daten: @dataArray ") if $logger->is_trace;
			$sth->execute(@dataArray) or do {
				my $errRec;
				for (my $j=0; $j < scalar(@{columns}); $j++) {
					my $datatype = $coldefs->{$columns[$j]}{"TYPE_NAME"}."(".$coldefs->{$columns[$j]}{"COLUMN_SIZE"}.")";
					$errRec.= $columns[$j]."[".$datatype."]:".$dataArray[$j].", ";
				}
				$logger->error($DBI::errstr.", bei Datensatz: $errRec");
				return 0;
			}
		}
	} else {
		$logger->error("no data to update");
		return 0;
	}
	return 1;
}
1;
__END__
=head1 NAME

=encoding CP1252

ETL::Wrap::DB - Database wrapper functions (for DBI / DBD::ODBC)

=head1 SYNOPSIS

 newDBH ($server, $database, $trusted)
 beginWork ()
 commit ()
 rollback ()
 readFromDB ($select)
 readFromDBHash ( $select, $keyfield)
 doInDB ( $doString, $retvals, @parameters)
 storeInDB ( $dbh, $tableName, $addID, $upsert, $primkey, 
 deleteFromDB ($data, $tableName, $keycol)
 updateInDB ($data, $tableName, $keycol)

=head1 DESCRIPTION

=item newDBH: erzeuge DBI Handle für Datenbankverbindung

 $server.. Datenbankserver
 $database .. Datenbank
 $trusted .. soll trusted connection Verbindungszeichenfolge verwendet werden (Datenbankverbindung mit aktuell eingeloggten user)?
 $longreadlen .. buffergrösse für Allokation großer Felder (siehe docstore.mik.ua/orelly/linux/dbi/ch06_01.htm)
 Rückgabe: DBI Handle zur weiteren Verwendung

=item beginWork: Datenbank Transaktionsbeginn

 $dbh .. DBI Handle für Transaktionsbeginn
 Rückgabe: 0 Bei Fehler, 1 wenn OK

=item commit: Datenbank Transaktionscommit

 $dbh .. DBI Handle für Transaktionscommit
 Rückgabe: 0 Bei Fehler, 1 wenn OK


=item rollback: Datenbank Transaktionsrollback

 $dbh .. DBI Handle für Transaktionsrollback
 Rückgabe: 0 Bei Fehler, 1 wenn OK


=item readFromDB: Dateneinholung aus Datenbank

 $dbh .. DBI Handle für Dateneinholung
 $select .. Abfragestring
 $columnnames .. optional Rückgabe der Feldnamen der Abfrage
 Rückgabe: ref auf array von hashwerten (wie von fetchall_arrayref zurückgegeben, also $return[zeilennummer, 0basiert]->{"<Feldname>"}). Bei Fehler Rückgabe von 0

=item readFromDBHash : Dateneinholung aus Datenbank

 $dbh .. DBI Handle für Dateneinholung
 $select .. Abfragestring
 $keyfield .. Feld, das im Abfragestring vorkommt, das für den hashkey der hashwerte verwendet werden soll.
 Rückgabe: ref auf hash von hashwerten (wie von selectall_hashref zurückgegeben, also $return->{hashkey}->{"<Feldname>"}). Bei Fehler Rückgabe von 0

=item doInDB: Allgemeine Ausführung eines SQL Statements in der Datenbank

 $dbh .. DBI Handle für Ausführung des SQL Statements
 $doString .. sql statement, das ausgeführt werden soll
 $retvals .. optional: Ref auf Array, das die Rückgabe(n) des $doStrings (üblicherweise stored proc) beinhalten wird.
 @parameters .. optional: wenn in $doString Platzhalter für parameter (?) enthalten sind, dann werden die parameter ab hier übergeben.
 Beispiel: DButil::doInDB($dbh,"sp_helpdb ?",\@retvals,"InfoDB"); oder DButil::doInDB($dbh,"sp_helpdb ?","","InfoDB") wenn Rückgabewerte nicht wichtig sind.
 Rückgabe: 0 Bei Fehler, 1 wenn OK

=item storeInDB: Datenablage in die Datenbank (insert oder "upsert")

 $data .. ref auf array von hashes, das in der Datenbank abgelegt werden soll:
 $data = [
           {
             'Feld1Name' => 'DS1Feld1Wert',
             'Feld2Name' => 'DS1Feld2Wert',
             ...
           },
           {
             'Feld1Name' => 'DS2Feld1Wert',
             'Feld2Name' => 'DS2Feld2Wert',
             ...
           },
         ];
 $dbh .. DBI Handle für Datenablage
 $tableName .. Tabelle, in der die Daten eingefügt werden sollen (kann auch ein vorangestelltes schema (mit "." getrennt) haben)
 $addID .. zusätzliches, konstantes ID-feld zu den daten geben (ref auf hash: {"NameOfIDField" => "valueOfIDField"}), nur eine ID möglich
 $upsert .. Datensatz aktualisieren, nachdem ein insert wegen bereits existentem primärschlüssel gescheitert ist (-> "upsert")
 $primkey .. WHERE Klausel (z.b. primID1 = ? AND primID2 = ?) zum Aufbau des update statements
 $ignoreDuplicateErrs .. Wenn $upsert nicht gesetzt wurde und duplikatsfehler bei insert ignoriert werden sollen
 $deleteBeforeInsertSelector .. WHERE Klausel (z.b. col1 = ? AND col2 = ?) zum Löschen existierender Daten vor der Ablage: Alle Daten, bei denen die Klausel für Werte im ersten Datensatz der abzulegenden Daten erfüllt ist,  werden gelöscht (Annahme, dass diese Werte für alle Datensätze gleich sind)
 $incrementalStore .. wenn gesetzt, werden undefinierte (NICHT Leer ("" !), sondern undef) Datenwerte NICHT auf NULL aktualisiert sondern beim insert/update statement übergangen
 $doUpdateBeforeInsert .. wenn gesetzt, dann wird bei upserts das update VOR dem Insert gemacht, das ist besonders bei Tabellen mit Identity Keys, wo das Kriterium auf einen anderen Schlüssel abstellt wichtig.
 $debugKeyIndicator .. Key Debug String (z.b. Key1 = ? Key2 = ?) zum Aufbau eines schlüsselhinweises bei Fehlermeldungen.
 Rückgabe: 0 Bei Fehler, 1 wenn OK

=item deleteFromDB: Daten in der Datenbank löschen

 $data.. ref auf hash von hasheinträgen (wie z.b. von selectall_hashref erhalten) mit schlüsselwerten von Datensätzen, die zu löschen sind
 $dbh .. DBI Handle für Datenlöschung
 $tableName .. Tabelle, in der die Daten gelöscht werden sollen
 $keycol .. Feldname bzw. WHERE Klausel (z.b. primID1 = ? AND primID2 = ?) um Daten, die gelöscht werden sollen, ausfindig zu machen. Ein "?" spezifiziert eine WHERE Klausel (wird nicht weiter ergänzt)
 Rückgabe: 0 Bei Fehler, 1 wenn OK

=item updateInDB: Aktualisiere Daten in der Datenbank

 $data.. ref auf hash von hasheinträgen (wie z.b. von selectall_hashref erhalten) mit schlüsselwerten von Datensätzen, die zu aktualisieren sind (keyval werte sind künstliche Schlüssel, die nicht für die Aktualisierung verwendet werden, sondern nur die Datensätze identifizieren)
 $data = { 'keyval1' =>
           {
             'Feld1Name' => 'DS1Feld1Wert',
             'Feld2Name' => 'DS1Feld2Wert',
             ...
           },
           'keyval2' => {
             'Feld1Name' => 'DS2Feld1Wert',
             'Feld2Name' => 'DS2Feld2Wert',
             ...
           },
         };
 $dbh .. DBI Handle für Datenaktualisierung
 $tableName .. Tabelle, in der die Daten aktualisiert werden sollen
 $keycol .. Feldname bzw. WHERE Klausel (z.b. primID1 = ? AND primID2 = ?) um Daten, die aktualisiert werden sollen, ausfindig zu machen. Ein "?" spezifiziert eine WHERE Klausel (wird nicht weiter ergänzt)
 Rückgabe: 0 Bei Fehler, 1 wenn OK

=head1 COPYRIGHT

Copyright (c) 2022 Roland Kapl

All rights reserved.  This program is free software; you can
redistribute it and/or modify it under the same terms as Perl itself.

The full text of the license can be found in the LICENSE file included
with this module.

=cut