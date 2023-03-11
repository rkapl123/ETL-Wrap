package ETL::Wrap;

our $VERSION = '0.1';

use strict;
use Log::Log4perl qw(get_logger); use Log::Log4perl::Level; use Time::Local; use Time::localtime; use MIME::Lite; use Data::Dumper; use Module::Refresh; use Exporter; use File::Copy; use Cwd;
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
our @EXPORT = qw(%common %config %execute @loads setupETLWrap removeFilesinFolderOlderX openDBConn openFTPConn redoFile getLocalFiles getFilesFromFTP checkFiles extractArchives getAdditionalDBData readFileData dumpDataIntoDB markProcessed writeFileFromDB executeUploadCMD uploadFileToFTP processingEnd processingPause retrySleepAbort moveFilesToHistory deleteFiles   get_logger   %months %monate get_curdate get_curdatetime get_curdate_dot formatDate formatDateFromYYYYMMDD get_curdate_dash get_curdate_gen get_curdate_dash_plus_X_years get_curtime get_curtime_HHMM get_lastdateYYYYMMDD get_lastdateDDMMYYYY is_first_day_of_month is_last_day_of_month get_last_day_of_month weekday is_weekend is_holiday first_week first_weekYYYYMMDD last_week last_weekYYYYMMDD convertDate convertDateFromMMM convertDateToMMM convertToDDMMYYYY addDays addDaysHol addMonths subtractDays subtractDaysHol convertcomma convertToThousendDecimal get_dateseries parseFromDDMMYYYY parseFromYYYYMMDD convertEpochToYYYYMMDD);

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
	ETL::Wrap::Common::getOptions(); # first get overriding command line options
	ETL::Wrap::Common::setupConfigMerge(); # %config (from site.config, amended with command line options) and %common (from process script) are merged into %common and all @loads
	ETL::Wrap::Common::setupLogging(\%common);
	# starting log entry: process script name + %common parameters, used for process monitoring (%config is not written due to sensitive information)
	$Data::Dumper::Indent = 0; # temporarily flatten dumper output for single line
	my $configdump = Dumper(\%common); 
	$Data::Dumper::Indent = 2;
	$configdump =~ s/\s+//g;$configdump =~ s/\$VAR1=//;$configdump =~ s/,'/,/g;$configdump =~ s/{'/{/g;$configdump =~ s/'=>/=>/g; # compress information
	get_logger()->info("==========================================================");
	get_logger()->info("started ".$execute{scriptname}.", parameters: ".$configdump);
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
	# only for set prefix, take username and password from $DB->{$DB->{prefix}}
	my ($user,$pwd);
	if ($DB->{prefix}) {
		$user = $config{sensitive}{$DB->{prefix}}{user};
		$pwd = $config{sensitive}{$DB->{prefix}}{pwd};
	}
	(!$DB->{user} && !$user && !$DB->{isTrusted}) and do {
		$logger->error("\$DB->{isTrusted} not set and user neither set in \$DB->{user} nor in \$config{sensitive}{".$DB->{prefix}."}{user} !");
		return 0;
	};
	do {
		ETL::Wrap::DB::newDBH($DB,\%execute,$user,$pwd) or do {
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
	# only for set prefix, take username and password from $FTP->{$FTP->{prefix}}
	my ($user,$pwd);
	if ($FTP->{prefix}) {
		$user = $config{sensitive}{$FTP->{prefix}}{user};
		$pwd = $config{sensitive}{$FTP->{prefix}}{pwd};
	}
	(!$FTP->{user} && !$user) and do {
		$logger->error("user neither set in \$FTP->{user} nor in \$config{sensitive}{".$FTP->{prefix}."}{user} !");
		return 0;
	};
	do {
		ETL::Wrap::FTP::login($FTP,\%execute,$user,$pwd) or do {
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
					push @{$execute{retrievedFiles}}, $_;
				}
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
		} else {
			$logger->error("no localFilesystemPath given in \$File parameter");
		}
	}
}

# get file/s (can also be a glob for multiple files) from FTP into homedir and extract archives if needed
sub getFilesFromFTP {
	my $arg = shift;
	my $logger = get_logger();
	my ($FTP,$File,$process) = ETL::Wrap::Common::extractConfigs($arg,"FTP","File","process");
	$logger->info("getFilesFromFTP");
	@{$execute{retrievedFiles}} = (); # reset last retrieved, but this is also necessary to create the retrievedFiles hash entry for passing back the list from getFiles
	@{$execute{filenames}} = (); # also reset last collected
	if ($process->{redoFile}) {
		redoFile($arg);
	} else {
		$logger->logdie("\$FTP{onlyDoFiletransferToLocalDir} given, but no \$FTP{localDir} set !") if !$FTP->{localDir} && $FTP->{onlyDoFiletransferToLocalDir};
		$logger->logdie("\$FTP{onlyArchive} given, but no \$FTP{archiveFolder} set !") if !$FTP->{archiveFolder} && $FTP->{onlyArchive};
		if ($File->{filename} && !$FTP->{onlyArchive}) {
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

# check files for continuation of process
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
		if ($execute{retrievedFiles} and @{$execute{retrievedFiles}} > 1) {
			for my $singleFilename (@{$execute{retrievedFiles}}) {
				$logger->debug("checking file: ".$redoDir.$singleFilename);
				open (CHECKFILE, "<".$redoDir.$singleFilename) or $fileDoesntExist=1;
				close CHECKFILE;
			}
		} else {
			$fileDoesntExist=1;
		}
	} else {
		# check single file
		$logger->debug("checking file: ".$redoDir.$File->{filename});
		open (CHECKFILE, "<".$redoDir.$File->{filename}) or $fileDoesntExist=1;
		close CHECKFILE;
	}
	if ($fileDoesntExist) {
		# exceptions from error message and return false for not continuing with readFile/whatever
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
	push @{$execute{filesToRemove}}, @{$execute{filenames}} if $FTP->{fileToRemove};
	push @{$execute{filesToArchive}}, @{$execute{filenames}} if $FTP->{fileToArchive};
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
	if ($File->{format_xlformat}) {
		$readSuccess = ETL::Wrap::File::readExcel($File, $process, $execute{filenames});
	} elsif ($File->{format_XML}) {
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
			# store data, tables are deleted unless explicitly marked
			unless ($DB->{keepContent}) {
				$logger->info("removing all data from Table $table ...");
				ETL::Wrap::DB::doInDB({doString => "delete from $table"});
			}
			$logger->info("dumping data to table $table");
			if (! ETL::Wrap::DB::storeInDB($DB, $process->{data})) {
				$logger->error("error storing DB data.. ");
				$process->{hadDBErrors}=1;
			}
			# post processing (Perl code) for config, where postDumpProcessing is defined
			if ($DB->{postDumpProcessing}) {
				$logger->info("starting postDumpProcessing");
				$logger->debug($DB->{postDumpProcessing});
				eval $DB->{postDumpProcessing};
				if ($@) {
					$logger->error("error ($@) in eval postDumpProcessing: ".$DB->{postDumpProcessing});
					$process->{hadDBErrors} = 1;
				}
			}
			# post processing (execute in DB!) for all configs, where postDumpExecs conditions and referred execs (DB scripts, that should be executed) are defined
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
							if ($exec) { # only if defined (there could be an interpolation of perl variables, if these are contained in $exec. This is for setting $selectedDate in postDumpProcessing.
								# eval qq{"$exec"} doesn't evaluate $exec but the quoted string (to enforce interpolation where needed)
								$exec = eval qq{"$exec"} if $exec =~ /$/; # only interpolate if perl scalars are contained
								$logger->info("post execute: $exec");
								if (!ETL::Wrap::DB::doInDB({doString => $exec})) {
									$logger->error("error executing postDumpExec: '".$exec."' .. ");
									$process->{hadDBErrors}=1;
								}
							}
						}
					}
				}
				$logger->info("postDumpExecs finished");
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
					$logger->info("Rollback because of error when storing into database");
					ETL::Wrap::DB::rollback() or $logger->error("error with rollback ...");
				}
				$logger->warn("error storing data into database");
				$execute{processFail} = 1;
			}
		}
	} else {# if ($process->{data}) .. in case there is no data and an empty file is OK no error will be thrown in readFile/readExcel, but any Import should not be done...
		if ($File->{emptyOK}) {
			$logger->warn("received empty file, will be ignored as \$File{emptyOK}=1");
		} else {
			$logger->error("error as one of the following files didn't contain data: @{$execute{filenames}} !");
		}
	}
}

# mark files as being processed depending on whether there were errors, also decide on removal/archiving of downloaded files
sub markProcessed {
	my $arg = shift;
	my $logger = get_logger();
	my ($File,$process) = ETL::Wrap::Common::extractConfigs($arg,"File","process");
	$logger->info("markProcessed");
	# this is important for the archival/deletion on the FTP Server!
	if ($File->{emptyOK} || !$process->{hadDBErrors}) {
		for (@{$execute{filenames}}) {
			$execute{filesProcessed}{$_} = 1;
			$logger->info("filesProcessed: $_");
		}
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
	# get data from database, including column names (passed by ref)
	$process->{data} = ETL::Wrap::DB::readFromDB($DB,\@columnnames) or $logger->error("couldn' read from DB");
	# pass column information from database, if not explicitly set
	$File->{columns} = \@columnnames if !$File->{columns};
	$logger->warn("no data retrieved") if (@{$process->{data}} == 0);
	# prepare for all configs, where postReadProcessing is defined
	if ($DB->{postReadProcessing}) {
		eval $DB->{postReadProcessing};
		$logger->error("error doing postReadProcessing: ".$DB->{postReadProcessing}.": ".$@) if ($@);
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
	ETL::Wrap::Common::setErrSubject("Uploading files with ".$process->{uploadCMD});
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
	# remove produced files
	for my $fileToWrite (@{$execute{filesToWrite}}) {
		unlink ($process->{uploadCMDPath}."/".$fileToWrite) or $logger->error("couldn't remove $fileToWrite in ".$process->{uploadCMDPath}.": ".$!);
	}
	# take error log from uploadCMD
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
sub uploadFileToFTP {
	my $arg = shift;
	my $logger = get_logger();
	my ($FTP,$File) = ETL::Wrap::Common::extractConfigs($arg,"FTP","File");
	$logger->info("uploadFileToFTP");
	ETL::Wrap::Common::setErrSubject("Upload of file to FTP");
	ETL::Wrap::FTP::uploadFile ($FTP,{fileToWrite => $File->{filename}}) or do {
		$logger->error("error uploading file to FTP...");
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
			$execute{processEnd} = 0; # reset, if repetition is planned
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

# general procedure for pausing processing
sub processingPause {
	my $arg = shift;
	my $logger = get_logger();
	$logger->info("pause");
	my $hrs = substr(ETL::Wrap::DateUtil::get_curtime_HHMM(),0,2);
	my $min = substr(ETL::Wrap::DateUtil::get_curtime_HHMM(),2,2);
	# Add time (60 module): 
	# hour part: including carry of minutes after adding additional minutes ($arg/60); * 100 for shifting 2 digits left
	# minute part: integer rest from 60 of (original + additional)
	my $nextStartTimeNum = ($hrs + int(($min+($arg/60))/60))*100 + (($min + ($arg/60))%60);
	$logger->info("pausing ".$arg." seconds, resume processing: ".sprintf("%04d",$nextStartTimeNum));
	sleep $arg;
}

# general retry procedure for retrying/aborting processing
sub retrySleepAbort {
	my $arg = shift;
	my $logger = get_logger();
	my ($process) = ETL::Wrap::Common::extractConfigs($arg,"process");
	$logger->info("retrySleepAbort");
	# time triggered finishing of retry-processing, special case midnight
	my $hrs = substr(ETL::Wrap::DateUtil::get_curtime_HHMM(),0,2);
	my $min = substr(ETL::Wrap::DateUtil::get_curtime_HHMM(),2,2);
	my $retrySeconds = $execute{retrySeconds};
	$retrySeconds = $process->{retrySecondsErr} if !$retrySeconds;
	# Add time (60 module): 
	# hour part: including carry of minutes after adding additional minutes ($retrySeconds/60); * 100 for shifting 2 digits left
	# minute part: integer rest from 60 of (original + additional)
	my $nextStartTimeNum = ($hrs + int(($min+($retrySeconds/60))/60))*100 + (($min + ($retrySeconds/60))%60);
	my $nextStartTime = sprintf("%04d",$nextStartTimeNum);
	my $currentTime = ETL::Wrap::DateUtil::get_curtime_HHMM();
	my $endTime = $process->{plannedUntil};
	$endTime = "0000->not set" if !$endTime;
	if ($currentTime >= $endTime or ($nextStartTime =~ /24../)) {
		$logger->info("finished processing due to time out: current time(".$currentTime.") >= endTime(".$endTime.") or nextStartTime(".$nextStartTime.") =~ /24../!");
		moveFilesToHistory($execute{filesToMoveinHistory},$process) if $execute{filesToMoveinHistory};
		deleteFiles($execute{filesToDelete},$process) if $execute{filesToDelete};
	} else {
		$logger->debug("execute:\n".Dumper(\%execute));
		$logger->info("Retrying in ".$retrySeconds." seconds because of ".($execute{retryBecauseOfError} ? "occurred error" : "planned retry")." until ".$endTime.", next run: ".$nextStartTime);
		sleep $retrySeconds;
	}
}

# moving files into history folder
sub moveFilesToHistory {
	my ($filenames,$process) = @_;
	my $logger = get_logger();
	my $cutOffDateTime = ETL::Wrap::DateUtil::get_curdatetime();
	my $redoDir = $process->{redoDir}."/" if $process->{redoFile};
	ETL::Wrap::Common::setErrSubject("lokale Archivierung/Bereinigung"); #
	for (@$filenames) {
		my ($strippedName, $ext) = /(.+)\.(.+?)$/;
		# if done from a redoDir, then add this folder to file (e.g. if done from redo/<username> then Filename_20190219_124409.txt becomes Filename_20190219_124409_redo_<username>_.txt)
		my $cutOffSpec = $cutOffDateTime;
		if ($redoDir) {
			my $redoSpec = $redoDir;
			$redoSpec =~ s/\//_/g;
			$cutOffSpec = $cutOffDateTime.'_'.$redoSpec;
		}
		if (!$execute{alreadyMovedOrDeleted}{$_}) {
			my $histTarget = $process->{historyFolder}."/".$strippedName."_".$cutOffSpec.".".$ext;
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

 %config .. hash for global config (set in $ENV{ETL_WRAP_CONFIG_PATH}/site.config, amended with $ENV{ETL_WRAP_CONFIG_PATH}/additional/*.config)
 %common .. common load configs for the process script
 @loads .. list of hashes defining specific load processes
 %execute .. hash of parameters for current process (having one or multiple loads)

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
=item moveFilesToHistory: move transferred files into history folder
 $filenames .. ref auf array von file names to be moved
 $process .. process context information
 
=item deleteFiles: delete transferred files
 $filenames .. ref to array of file names to be deleted
 $process .. process context information

=head1 COPYRIGHT

Copyright (c) 2023 Roland Kapl

All rights reserved.  This program is free software; you can
redistribute it and/or modify it under the same terms as Perl itself.

The full text of the license can be found in the LICENSE file included
with this module.

=cut