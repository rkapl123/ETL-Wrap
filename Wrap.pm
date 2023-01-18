package ETL::Wrap;

our $VERSION = '0.1';

use strict;
use Log::Log4perl qw(get_logger); use Log::Log4perl::Level; use Time::Local; use Time::localtime; use MIME::Lite; use Data::Dumper; use Module::Refresh; use Exporter;
# we make $ETL::Wrap::Common::common/config/execute/loads an alias for $ETL::Wrap::common/config/execute/loads so that the user can set it without knowing anything about the Common package!
our %common;
our %config;
our %execute;
our @loads;
BEGIN {
	*ETL::Wrap::Common::common = \%common;
	*ETL::Wrap::Common::config = \%config;
	*ETL::Wrap::Common::execute = \%execute;
	*ETL::Wrap::Common::loads = \@loads;
};
use ETL::Wrap::Common; use ETL::Wrap::DateUtil; use ETL::Wrap::DB; use ETL::Wrap::File; use ETL::Wrap::FTP;

our @ISA = qw(Exporter);
our @EXPORT = qw(%common %config %execute @loads setupETLWrap);

# initialize module, reading all config files and setting basic execution variables
sub INIT {
	# read site config and additional configs in alphabetical order (allowing precedence)
	readConfigFile($ENV{ETL_WRAP_CONFIG_PATH}."/site.config");
	readConfigFile($_) for sort glob($ENV{ETL_WRAP_CONFIG_PATH}."/additional/*.config");
	
	$execute{homedir} = File::Basename::dirname(File::Spec->rel2abs((caller(0))[1])); # folder, where the main script is being executed.
	$execute{scriptname} = File::Basename::fileparse((caller(0))[1],(".pl"));
	my ($homedirnode) = ($execute{homedir} =~ /^.*[\\\/](.*?)$/);
	$execute{envraw} = $config{folderEnvironmentMapping}{$homedirnode};
	if ($execute{envraw}) {
		$execute{env} = $execute{envraw};
	} else {
		# if not configured, used default mapping (usually ''=>"Prod" for productionn)
		$execute{env} = $config{folderEnvironmentMapping}{''};
	}
	if ($execute{envraw}) { # for non-production environment read separate config, if existing
		readConfigFile($ENV{ETL_WRAP_CONFIG_PATH}."/".$execute{envraw}."/site.config") if -e $ENV{ETL_WRAP_CONFIG_PATH}."/".$execute{envraw}."/site.config";
		readConfigFile($_) for sort glob($ENV{ETL_WRAP_CONFIG_PATH}."/".$execute{envraw}."/additional/*.config");
	}
}

# read given config file (eval perl code)
sub readConfigFile {
	my ($configfilename) = @_;
	my $siteCONFIGFILE;
	open (CONFIGFILE, "<$configfilename") or die("couldn't open $configfilename: $@ $!, caller ".(caller(1))[3].", line ".(caller(1))[2]." in ".(caller(1))[1]);
	{
		local $/=undef;
		$siteCONFIGFILE = <CONFIGFILE>;
		close CONFIGFILE;
	}
	unless (my $return = eval $siteCONFIGFILE) {
		die("Error parsing config file $configfilename : $@") if $@;
		die("Error executing config file $configfilename : $!") unless defined $return;
		die("Error executing config file $configfilename") unless $return;
	}
}

# set up ETL configuration
sub setupETLWrap {
	ETL::Wrap::Common::getOptions();
	ETL::Wrap::Common::setupConfigMerge();
	ETL::Wrap::Common::setupLogging(\%common);
	ETL::Wrap::Common::setupStarting(\%common);
	ETL::Wrap::Common::checkHash(\%config,"config");
	ETL::Wrap::Common::checkHash(\%common,"common");
}

# remove all files in FTP server folders that are older than a given day/month/year
sub removeFilesinFolderOlderX {
	my $arg = shift;
	my $logger = get_logger();
	my ($FTP) = ETL::Wrap::Common::extractConfigs($arg,"FTP");

	ETL::Wrap::Common::setErrSubject("Cleaning of Archive folders");
	ETL::Wrap::FTP::removeFilesOlderX($FTP) or $logger->error("error removing archived files");
}

# open a DB connection
sub openDBConn {
	my $arg = shift;
	my $logger = get_logger();
	my ($DB,$process) = ETL::Wrap::Common::extractConfigs($arg,"DB","process");
	my $exitTry = 0;
	$logger->info("openDBConn");
	do {
		ETL::Wrap::DB::newDBH($DB) or do {
			$exitTry = 1;
			$logger->error("couldn't open database connection, DB config:\n".Dumper($DB));
			retrySleepAbort($arg);
			$exitTry = 0 if $execute{processEnd};
		};
	} while ($exitTry);
	return !$execute{processEnd}; # false means error in connection and signal to die...
}

# open a FTP connection
sub openFTPConn {
	my $arg = shift;
	my $logger = get_logger();
	my ($FTP,$process) = ETL::Wrap::Common::extractConfigs($arg,"FTP","process");
	my $exitTry = 0;
	$logger->info("openFTPConn");
	do {
		ETL::Wrap::FTP::login($FTP,\%execute) or do {
			$exitTry = 1;
			$logger->error("couldn't open ftp connection, FTP config:\n".Dumper($FTP));
			retrySleepAbort($arg);
			$exitTry = 0 if $execute{processEnd};
		};
	} while ($exitTry);
	return !$execute{processEnd}; # false means error in connection and signal to die...
}

# redo file from redo directory if specified (used in getLocalFile and getFileFromFTP)
sub redoFile {
	my $arg = shift;
	my $logger = get_logger();
	my ($File,$process) = ETL::Wrap::Common::extractConfigs($arg,"File","process");
	my $redoDir = $process->{redoDir};
	$logger->info("redoFile files in ".$redoDir." ...");
	ETL::Wrap::Common::setErrSubject("setting/renaming redo files");
	# file extension for local redo 
	my ($ext) = $File->{filename} =~ /.*\.(.*?)$/; # get file extension from filename
	if (!$ext) {
		$ext = $File->{extension}; # if no dots in filename (e.g. because of glob) -> no file extension retrievable -> take from here
	}
	$logger->error("no file extension for renaming redo files! should be either retrievable in filename as .<ext> or separately in extension tag!") if (!$ext);
	if (chdir($redoDir)) {
		for (glob("*.$ext")) {
			my $newName;
			if (($newName) = /^(.+?)(\d{14}|_\d{8}|_\d{8}_\d{6}|_\d{8}_\d{6}_)?\.$ext$/) {
				if ($File->{filename} =~ /$newName.*\.$ext$/ && !$File->{avoidRenameForRedo}) {
					$logger->info("available for redo: ".$_.", renamed: $newName.$ext");
					rename $_, "$newName.$ext" or $logger->error("error renaming file $_ to $newName.$ext : $!");
					$_ = "$newName.$ext";
					s/^.*\///;
				}
				$execute{redoFiles}{$_}=1;
			}
		}
		extractArchives($arg) if ($File->{extract});
	} else {
		$logger->error("couldn't change into redo folder ".$redoDir." !");
	}
	chdir($execute{homedir});
}

# get local file(s) from source into homedir and extract archives if needed
sub getLocalFiles {
	my $arg = shift;
	my $logger = get_logger();
	my ($File,$process) = ETL::Wrap::Common::extractConfigs($arg,"File","process");
	$logger->info("getLocalFiles");
	if ($process->{redoFile}) {
		redoFile($arg);
	} else {
		if ($File->{localFilesystemPath}) {
			my @multipleFiles;
			if ($File->{filename} =~ /\*/) { # if there is a glob character then copy multiple files !
				@multipleFiles = glob($File->{localFilesystemPath}.$File->{filename}); # list retrieved files for later processing, names_only => 1 for getting only the filename
				push @{$execute{retrievedFiles}}, @multipleFiles;
			} else {
				# no glob char -> single file
				push @multipleFiles, $File->{filename};
			}
			push @{$execute{retrievedFiles}}, @multipleFiles;
			for my $localfile (@multipleFiles) {
				$logger->info("copying local file: ".$File->{localFilesystemPath}.$localfile);
				copy ($File->{localFilesystemPath}.$localfile, ".") or $logger->error("couldn't copy ".$File->{localFilesystemPath}.$localfile.": $!");
			}
			extractArchives($arg) if ($File->{extract});
		}
	}
}

# get file(s) from FTP into homedir and extract archives if needed
sub getFilesFromFTP {
	my $arg = shift;
	my $logger = get_logger();
	my ($FTP,$File,$process) = ETL::Wrap::Common::extractConfigs($arg,"FTP","File","process");
	$logger->info("getFilesFromFTP");
	if ($process->{redoFile}) {
		redoFile($arg);
	} else {
		$logger->logdie("\$FTP{onlyDoFiletransferToLocalDir} given, but no \$FTP{localDir} set !") if !$FTP->{localDir} && $FTP->{onlyDoFiletransferToLocalDir};
		$logger->logdie("\$FTP{onlyArchive} given, but no \$FTP{ArchiveFolder} set !") if !$FTP->{ArchiveFolder} && $FTP->{onlyArchive};
		@{$execute{retrievedFiles}} = (); # reset lost, but this is also necessary to create the retrievedFiles hash entry for passing back the list from getFiles
		if ($File->{filename} && !$FTP->{onlyArchive}) {
			$logger->info("fetching ".$File->{filename}." from FTP");
			if (!ETL::Wrap::FTP::getFiles ($FTP,\%execute,{fileToRetrieve=>$File->{filename},fileToRetrieveOptional=>$File->{optional}})) {
				$logger->error("error in fetching file from FTP") if !$execute{retryBecauseOfError};
			} else {
				if ($File->{extract} && @{$execute{retrievedFiles}} == 1) {
					extractArchives($arg);
				} else {
					$logger->error("multiple files returned (glob passed as filename), extracting not supported here") if $File->{extract};
				}
			}
		}
	}
}

# check files for continuation
sub checkFiles {
	my $arg = shift;
	my $logger = get_logger();
	my ($FTP,$File,$process) = ETL::Wrap::Common::extractConfigs($arg,"FTP","File","process");
	$logger->info("checkFiles");
	my $redoDir = $process->{redoDir}."/" if $process->{redoFile};
	my $fileDoesntExist;
	# check globs
	if ($File->{filename} =~ /\*/) {
		#  check files fetched with globs
		if ($execute{retrievedFiles} && @{$execute{retrievedFiles}} > 1) {
			for my $singleFilename (@{$execute{retrievedFiles}}) {
				$logger->debug("checking file retrieved with mget: ".$singleFilename);
				open (CHECKFILE, "<".$singleFilename) or $fileDoesntExist=1;
				close CHECKFILE;
			}
		} else {
			$fileDoesntExist=1;
		}
	} else {
		# check single file
		$logger->debug("checking file ".$redoDir.$File->{filename});
		open (CHECKFILE, "<".$redoDir.$File->{filename}) or $fileDoesntExist=1;
		close CHECKFILE;
	}
	if ($fileDoesntExist) {
		# exceptions from error message and return false for not continuing with readFile
		if ($File->{optional} || ($execute{firstRunSuccess} && $process->{plannedUntil}) || $process->{redoFile}) {
			if ($execute{firstRunSuccess} && $process->{plannedUntil}) {
				$logger->warn("file ".$File->{filename}." missing with planned execution until ".$process->{plannedUntil}." and first run successful, skipping");
			} elsif ($File->{optional}) {
				$logger->warn("file ".$File->{filename}." missing being marked as optional, skipping");
			} elsif ($process->{redoFile}) {
				$logger->warn("file ".$File->{filename}." missing being retried, skipping");
			}
		} else {
			$logger->error("file ".$File->{filename}." doesn't exist and is not marked as optional!");
			$execute{processFail} = 1;
		}
		return 0;
	}
	push @{$execute{filenames}}, @{$execute{retrievedFiles}} if $execute{retrievedFiles} && @{$execute{retrievedFiles}} > 0; # add the files retrieved with mget here.
	push @{$execute{filesToRemove}}, @{$execute{filenames}} if $FTP->{filenameToRemove};
	push @{$execute{filesToArchive}}, @{$execute{filenames}} if $FTP->{filenameToArchive};
	# check, ob files or file globs exist when redoing
	if ($process->{redoFile}) {
		my $redoGlob = $File->{filename} if $File->{filename} =~ /\*/;
		for my $aRedoFile (@{$execute{filenames}}) {
			if (!$execute{redoFiles}{$aRedoFile}) {
				$logger->warn("file ".$aRedoFile." not contained in redo files, therefore skipping in redo");
				return 0;
			}
		}
		if (!glob($redoDir.$redoGlob)) {
			$logger->warn("no files exist in ".$redoDir.$redoGlob.", therefore skipping in redo");
			return 0;
		} else {
			chdir($redoDir);
			# resolve glob in redo folder and push into filenames to be processed
			push @{$execute{filenames}}, glob("$redoGlob");
			chdir($execute{homedir});
		}
	}
	return 1;
}

# extract files from archive
sub extractArchives {
	my $arg = shift;
	my $logger = get_logger();
	my ($File,$process) = ETL::Wrap::Common::extractConfigs($arg,"File","process");
	$logger->info("extractArchives");
	my $redoDir = $process->{redoDir}."/" if $process->{redoFile};
	if ($File->{extract}) { # extract files from archive package (zip)
		$logger->info("extracting file(s) from archive package: ".$redoDir.$File->{filename});
		my $ae = Archive::Extract->new(archive => $redoDir.$File->{filename});
		$ae->extract(to => ($redoDir ? $redoDir : ".")) or $logger->error("error extracting files: ".$ae->error());
		$logger->info("extracted files: ".@{$ae->files});
		push @{$execute{filenames}}, @{$ae->files};
		push @{$execute{archivefilenames}}, $File->{filename}; # archive itself needs to be removed/historized
	}
}

# get additional data from DB
sub getAdditionalDBData {
	my $arg = shift;
	my $logger = get_logger();
	my ($DB) = ETL::Wrap::Common::extractConfigs($arg,"DB");
	$logger->info("getAdditionalDBData");
	# additional lookup needed (e.g. used in addtlProcessing)?
	ETL::Wrap::DB::readFromDBHash($DB, $execute{additionalLookupData}) if ($DB->{additionalLookup});
}

# read data from file
sub readFileData {
	my $arg = shift;
	my $logger = get_logger();
	my ($File,$process) = ETL::Wrap::Common::extractConfigs($arg,"File","process");
	$logger->info("readFileData");
	my $readSuccess;
	if ($File->{format}{xlsx} or $File->{format}{xls}) {
		$readSuccess = ETL::Wrap::File::readExcel($File, $process, $execute{filenames});
	} elsif ($File->{format}{XML}) {
		$readSuccess = ETL::Wrap::File::readXML($File, $process, $execute{filenames});
	} else {
		$readSuccess = ETL::Wrap::File::readText($File, $process, $execute{filenames});
	}
	# error when reading files with readFile/readExcel/readXML
	if (!$readSuccess) {
		$logger->error("error reading one of file(s) ".@{$execute{filenames}}) if (!$File->{optional});
		$logger->warn("error reading one of file(s) ".@{$execute{filenames}}.', is ignored as $File{emptyOK} = 1!') if ($File->{emptyOK});
	}
	return $readSuccess;
}

# store data into Database
sub dumpDataIntoDB {
	my $arg = shift;
	my $logger = get_logger();
	my ($DB,$File,$process) = ETL::Wrap::Common::extractConfigs($arg,"DB","File","process");
	$logger->info("dumpDataIntoDB");
	$process->{hadDBErrors}=0;
	if ($process->{data}) { # data supplied?
		if ($DB->{noDumpIntoDB}) {
			$logger->info("skip dumping of ".$File->{filename}." into DB");
		} else {
			my $table = $DB->{tablename};
			# Transaction begin
			unless ($DB->{noDBTransaction}) {
				ETL::Wrap::DB::beginWork() or do {
					$logger->error ("couldn't start DB transaction");
					$process->{hadDBErrors}=1;
				};
			}
			# Allgemein: Abspeichern der Daten, bis auf explizit markierte werden Tabellen vorher gelöscht !!
			unless ($DB->{keepContent}) {
				$logger->info("Entferne alle daten von Tabelle $table ...");
				ETL::Wrap::DB::doInDB("delete from $table");
			}
			$logger->info("dumping data to table $table");
			if (! ETL::Wrap::DB::storeInDB($DB, $process->{data})) {
				$logger->error("error storing DB data.. ");
				$process->{hadDBErrors}=1;
			}
			# Nachbehandlung für alle configs, in denen postDumpProcessing definiert ist.
			if ($process->{postDumpProcessing}) {
				$logger->info("starting postDumpProcessing");
				$logger->debug($process->{postDumpProcessing});
				eval $process->{postDumpProcessing};
				if ($@) {
					$logger->error("Fehler ($@) in eval postDumpProcessing: ".$process->{postDumpProcessing});
					$process->{hadDBErrors} = 1;
				}
			}
			# Nachbehandlung für alle configs, in denen postDumpExecs conditions 
			# (Bedingung, unter der die einzelnen execs ausgeführt werden sollen) + zugehörige execs (DB scripts, die bei Erfüllung ausgeführt werden sollen)
			# definiert sind.
			if (!$process->{hadDBErrors} && $DB->{postDumpExecs}) {
				$logger->info("starting postDumpExecs ... ");
				for my $postDumpExec (@{$DB->{postDumpExecs}}) {
					$logger->info("checking postDumpExec condition: ".$postDumpExec->{condition});
					my $dopostdumpexec = eval $postDumpExec->{condition};
					if ($@) {
						$logger->error("error ($@) parsing postDumpExec condition: ".$postDumpExec->{condition});
						$process->{hadDBErrors} = 1;
						next;
					}
					if ($dopostdumpexec) {
						for my $exec (@{$postDumpExec->{execs}}) {
							if ($exec) { # nur wenn definiert (auch im exec können 
								# interpolieren von perl variablen, sofern sie in $exec vorkommen. Das dient zb zum setzen von $selectedDate in postDumpProcessing vorher.
								# eval qq{"$exec"} evaluiert nicht $exec sondern nur den quotierten string
								$exec = eval qq{"$exec"} if $exec =~ /$/;
								$logger->info("post execute: $exec");
								if (!ETL::Wrap::DB::doInDB($exec)) {
									$logger->error("error executing postDumpExec: '".$exec."' .. ");
									$process->{hadDBErrors}=1;
								}
							}
						}
					}
				}
				$logger->info("postDumpExecs erledigt");
			}
			if (!$process->{hadDBErrors}) {
				# Transaction: commit of DB changes
				unless ($DB->{noDBTransaction}) {
					$logger->debug("committing data");
					if (ETL::Wrap::DB::commit()) {
						$logger->info("data stored into table $table successfully");
					} else {
						$logger->error("error when committing");
						$process->{hadDBErrors}=1;
					};
				}
			} else { # error dumping to DB or during pre/postDumpExecs
				unless ($DB->{noDBTransaction}) {
					$logger->info("Rollback aufgrund Fehlers beim Ablegen in die Datenbank !");
					ETL::Wrap::DB::rollback() or $logger->error("Fehler beim rollback ...");
				}
				$logger->warn("error storing data into database");
				$execute{processFail} = 1;
			}
		}
	} else {# if ($process->{data}) .. in case there is no data and an empty file is OK no error will be thrown in readFile/readExcel, but any Import should not be done...
		if ($File->{emptyOK}) {
			$logger->warn('received empty file, will be ignored as $File{emptyOK}=1');
		} else {
			$logger->error("error as one of the files didn't contain data: ".@{$execute{filenames}}." !");
		}
	}
}

# mark files as being processed depending on whether there were errors, also decide on removal/archiving of downloaded files
sub markProcessed {
	my $arg = shift;
	my $logger = get_logger();
	my ($File,$process) = ETL::Wrap::Common::extractConfigs($arg,"File","process");
	$logger->info("markProcessed");
	if ($File->{emptyOK} || !$process->{hadDBErrors}) {
		$execute{filesProcessed}{$_} = 1 for @{$execute{filenames}};
	}
	# mark to be removed or be moved to history
	if ($File->{dontKeepHistory}) {
		push @{$execute{filesToDelete}}, @{$execute{filenames}};
		push @{$execute{filesToDelete}}, @{$execute{archivefilenames}} if $execute{archivefilenames};
	} else {
		push @{$execute{filesToMoveinHistory}}, @{$execute{filenames}};
		push @{$execute{filesToMoveinHistory}}, @{$execute{archivefilenames}} if $execute{archivefilenames};
	}
}

# create Data-files from Database
sub writeFileFromDB {
	my $arg = shift;
	my $logger = get_logger();
	my ($DB,$File,$FTP,$process) = ETL::Wrap::Common::extractConfigs($arg,"DB","File","FTP","process");
	$logger->info("writeFileFromDB");
	ETL::Wrap::Common::setErrSubject("reading files from DB");
	my @columnnames;
	# Daten von der Datenbank holen, jetzt inkl. Spaltennamen !
	$process->{data} = ETL::Wrap::DB::readFromDB($DB,\@columnnames) or $logger->error("couldn' read from DB");
	# Durchreichen an columns Information aus der Datenbank, wenn nicht explizit gesetzt...
	$File->{columns} = \@columnnames if !$File->{columns};
	$logger->warn("no data retrieved") if (@{$process->{data}} == 0);
	# Vorbehandlung für alle configs, in denen postReadProcessing definiert ist.
	if ($process->{postReadProcessing}) {
		eval $process->{postReadProcessing};
		$logger->error("error doing postReadProcessing: ".$process->{postReadProcessing}.": ".$@) if ($@);
	}
	ETL::Wrap::Common::setErrSubject("creating/writing files");
	$logger->error("error creating/writing file") if !ETL::Wrap::File::writeText($File,$process);
	if (($FTP->{remoteDir} and $FTP->{remoteHost}{$execute{env}}) or $process->{uploadCMD}) {
		$logger->debug ("preparing upload of file '".$File->{filename}."' using ".($process->{uploadCMD} ? $process->{uploadCMD} : "FTP"));
		push @{$execute{filesToWrite}}, $File->{filename};
	}
	# archive independently of upload
	push @{$execute{filesToArchive}}, $File->{filename};
	$logger->warn("no FTP remoteDir defined, therefore no files to FTP") if (!$FTP->{remoteDir} or !$FTP->{remoteHost}{$execute{env}});
	if ($FTP->{localDir}) {
		move($File->{filename}, $FTP->{localDir}."/".$File->{filename}) or $logger->error("couldn't move ".$File->{filename}." into ".$FTP->{localDir}.": ".$!);
	}
}

# "upload" files using an upload command program
sub executeUploadCMD {
	my $arg = shift;
	my $logger = get_logger();
	my ($File,$process) = ETL::Wrap::Common::extractConfigs($arg,"File","process");
	$logger->info("executeUploadCMD");
	ETL::Wrap::Common::setErrSubject("Hinaufladen der Dateien mit ".$process->{uploadCMD});
	system $process->{uploadCMD};
	if ($? == -1) {
		$logger->error($process->{uploadCMD}." failed: $!");
	} elsif ($? & 127) {
		$logger->error($process->{uploadCMD}." unexpected finished returning ".($? & 127).", ".(($? & 128) ? 'with' : 'without')." coredump");
	} elsif ($? != 0) {
		$logger->error($process->{uploadCMD}." finished returning ".($? >> 8).", err: $!");
	} else {
		$logger->info("finished upload using ".$process->{uploadCMD});
	}
	# zusammenräumen
	for my $fileToWrite (@{$execute{filesToWrite}}) {
		unlink ($process->{uploadCMDPath}."/".$fileToWrite) or $logger->error("couldn't remove $fileToWrite in ".$process->{uploadCMDPath}.": ".$!);
	}
	# Fehlerlog von uploadCMD übernehmen
	if (-e $process->{uploadCMDLogfile}) {
		my $err = do {
			local $/ = undef;
			open (FHERR, "<".$process->{uploadCMDLogfile}) or $logger->error("couldn't read uploadCMD log ".$process->{uploadCMDLogfile}.":".$!);
			<FHERR>;
		};
		$logger->error($process->{uploadCMD}." returned following: $err");
	}
}

# upload files to FTP
sub uploadFilesToFTP {
	my $arg = shift;
	my $logger = get_logger();
	my ($FTP,$File,$process) = ETL::Wrap::Common::extractConfigs($arg,"FTP","File","process");
	$logger->info("uploadFilesToFTP");
	ETL::Wrap::Common::setErrSubject("Hinaufladen der Dateien mit FTP");
	ETL::Wrap::FTP::writeFiles ({
		filesToWrite => $execute{filesToWrite}
	}) or do {
		$logger->error("Fehler beim upload von dateien mit FTP...");
	};
}

# final processing steps for processEnd (cleanup, FTP removal/archiving) or retry after pausing
sub processingEnd {
	my $arg = shift;
	my $logger = get_logger();
	my ($File,$FTP,$process) = ETL::Wrap::Common::extractConfigs($arg,"File","FTP","process");
	$logger->info("processingEnd");
	$execute{processEnd} = 1;
	for my $process (@loads) {
		$execute{processEnd} = 0 if $process->{hadDBErrors} and !$process->{File}{emptyOK} and !$process->{File}{optional};
	}
	my $redoDir = $process->{redoDir}."/" if $process->{redoFile};
	if ($execute{processEnd}) {
		ETL::Wrap::Common::setErrSubject("FTP/local archiving/removal");
		# only pass the actual processed files for archiving
		my @filesToRemove = grep {$execute{filesProcessed}{$_}} @{$execute{filesToRemove}} if $execute{filesToRemove};
		my @filesToArchive = grep {$execute{filesProcessed}{$_}} @{$execute{filesToArchive}} if $execute{filesToArchive};
		# when "onlyArchive" files are not processed, so pass all
		@filesToRemove = @{$execute{filesToRemove}} if $FTP->{onlyArchive} and $execute{filesToRemove}; 
		@filesToArchive = @{$execute{filesToArchive}} if $FTP->{onlyArchive} and $execute{filesToArchive};
		$logger->debug("files to remove: @filesToRemove");
		$logger->debug("files to archive: @filesToArchive");
		# archiving/removing on the FTP server only if not a local redo and there are actually files to remove/archive
		if (!$process->{redoFile} && (@filesToRemove > 0 || @filesToArchive > 0)) {
			$logger->info("file cleanup: ".(@filesToArchive ? "archiving @filesToArchive" : "").(@filesToRemove ? "removing @filesToRemove" : "")." on FTP Server...");
			ETL::Wrap::FTP::archiveFiles ($FTP,
			{
				filesToRemove => \@filesToRemove,
				filesToArchive => \@filesToArchive,
			}) or $logger->error("error cleaning up files on FTP: $!");
		}
		# clean up locally
		moveFilesToHistory($execute{filesToMoveinHistory},$process);
		deleteFiles($execute{filesToDelete},$process);
		if ($process->{plannedUntil}) {
			$execute{processEnd} = 0; # zurücksetzen, wenn endzeit in Optionen mitgegeben (z.b. bei Collateral files)
			$execute{retrySeconds} = $process->{retrySecondsPlanned};
		}
		if ($execute{retryBecauseOfError}) {
			# send success mail, if successful after first failure
			ETL::Wrap::Common::sendSuccessMail($File->{filename}, $execute{scriptname}.($process->{typeconf} ? "(".$process->{typeconf}.($process->{subtypeconf} ? ",".$process->{subtypeconf} : "").")" : ""));
		}
		$execute{firstRunSuccess} = 1 if $process->{plannedUntil}; # for planned retries (plannedUntil) -> no more error messages (files might be gone)
		$execute{retryBecauseOfError} = 0;
	} else {
		if ($process->{plannedUntil} && $execute{firstRunSuccess}) {
			$execute{retrySeconds} = $process->{retrySecondsPlanned};
		} else {
			$execute{retrySeconds} = $process->{retrySecondsErr};
			$execute{retryBecauseOfError} = 1;
		}
		# reset additionalLookupData to avoid strange errors in retrying run.
		undef $execute{additionalLookupData};
	}
	# refresh modules to enable correction of processing without restart
	Module::Refresh->refresh;
	# also check for changes in logging configuration
	Log::Log4perl::init($ENV{ETL_WRAP_CONFIG_PATH}."/".$execute{envraw}."/log.config"); # environment dependent log config, Prod is in ETL_WRAP_CONFIG_PATH
	# pausing processing
	retrySleepAbort($arg);
}

# general retry procedure for pausing processing
sub retrySleepAbort {
	my $arg = shift;
	my $logger = get_logger();
	my ($process) = ETL::Wrap::Common::extractConfigs($arg,"process");
	# time triggered finishing of retry-processing, special case midnight
	my $hrs = substr(get_curtime_HHMM(),0,2);
	my $min = substr(get_curtime_HHMM(),2,2);
	my $retrySeconds = $execute{retrySeconds};
	$retrySeconds = $process->{retrySecondsErr} if !$retrySeconds;
	# Add time (60 module): 
	# hour part: including carry of minutes after adding additional minutes ($retrySeconds/60); * 100 for shifting 2 digits left
	# minute part: integer rest from 60 of (original + additional)
	my $nextStartTimeNum = ($hrs + int(($min+($retrySeconds/60))/60))*100 + (($min + ($retrySeconds/60))%60);
	$execute{nextStartTime} = sprintf("%04d",$nextStartTimeNum);
	my $currentTime = get_curtime_HHMM();
	my $endTime = $process->{plannedUntil};
	$endTime = "0000->not set" if !$endTime;
	if ($currentTime >= $endTime or ($execute{nextStartTime} =~ /24../)) {
		$logger->info("finished processing due to time out: current time(".$currentTime.") >= endTime(".$endTime.") or nextStartTime(".$execute{nextStartTime}.") =~ /24../!");
		moveFilesToHistory($execute{filesToMoveinHistory},$process) if $execute{filesToMoveinHistory};
		deleteFiles($execute{filesToDelete},$process) if $execute{filesToDelete};
	} else {
		$logger->debug("execute:\n".Dumper(\%execute));
		$logger->info("Retrying in ".$retrySeconds." seconds because of ".($execute{retryBecauseOfError} ? "occurred error" : "planned retry")." until ".$endTime.", next run: ".$execute{nextStartTime});
		sleep $retrySeconds;
	}
}

# moving files into history folder
sub moveFilesToHistory {
	my ($filenames,$process) = @_;
	my $logger = get_logger();
	my $cutOffExt = ETL::Wrap::DateUtil::get_curdatetime();
	my $redoDir = $process->{redoDir}."/" if $process->{redoFile};
	ETL::Wrap::Common::setErrSubject("lokale Archivierung/Bereinigung"); #
	for (@$filenames) {
		my ($histName, $ext) = /(.+)\.(.+?)$/;
		# if done for redoDir, then add redo_<UserspecificFolder> to file histname (e.g. Filename_20190219_124409.txt becomes Filename_20190219_124409_redo_<username>_.txt)
		if ($redoDir) {
			my $redoExt = $redoDir;
			$redoExt =~ s/\//_/g;
			$cutOffExt .= '_'.$redoExt;
		}
		if (!$execute{alreadyMovedOrDeleted}{$_}) {
			my $histTarget = $process->{HistoryDir}."/".$histName."_".$cutOffExt.$ext;
			$logger->info("moving file $redoDir$_ into $histTarget");
			rename $redoDir.$_, $histTarget or $logger->error("error when moving file $redoDir$_ into $histTarget: $!");
			$execute{alreadyMovedOrDeleted}{$_} = 1;
		}
	}
}

# removing files
sub deleteFiles {
	my ($filenames,$process) = @_;
	my $logger = get_logger();
	my $redoDir = $process->{redoDir}."/" if $process->{redoFile};
	for (@$filenames) {
		if (!$execute{alreadyMovedOrDeleted}{$_}) {
			$logger->info("removing ".($process->{redoFile} ? "repeated loaded " : "")."file $redoDir$_ ");
			unlink $redoDir.$_ or $logger->error("error when removing file $redoDir".$_." : $!");
			$execute{alreadyMovedOrDeleted}{$_} = 1;
		}
	}
}
1;
__END__

=head1 NAME

ETL::Wrap - Package wrapping tasks for ETL

=head1 SYNOPSIS

 %config .. hash for global config (set in site.config and additional folder)
 %common .. common configs for loads
 @loads .. array of hash of hashes defining loads

=head1 DESCRIPTION

=item setupETLWrap
=item removeFilesinFolderOlderX
=item redoFiles
=item readLocalFile
=item getFilesFromFTP
=item openDBConn
=item checkFiles
=item extractArchives
=item changeDBgetAdditionalDBData
=item readFileData
=item dumpDataIntoDB
=item processingEnd
=item retrySleepAbort
=item moveFilesToHistory: Datenfiles in History verschieben
 $filenames .. ref auf array von filenamen, die zu verschieben sind
 $redoDir .. redo Verzeichnis, falls Wiederholung
 
=item deleteFiles: Datenfiles löschen
 $filenames .. ref auf array von filenamen, die zu löschen sind
 $redoDir .. redo Verzeichnis, falls Wiederholung

=head1 COPYRIGHT

Copyright (c) 2022 Roland Kapl

All rights reserved.  This program is free software; you can
redistribute it and/or modify it under the same terms as Perl itself.

The full text of the license can be found in the LICENSE file included
with this module.

=cut