package ETL::Wrap::Common;

use strict;
use Exporter; use Log::Log4perl qw(get_logger); use ETL::Wrap::DateUtil; use Data::Dumper; use Getopt::Long qw(:config no_ignore_case); use Scalar::Util qw(looks_like_number);
# to make use of colored logs with Log::Log4perl::Appender::ScreenColoredLevels on windows we have to use that (special "use" to make this optional on non-win environments)
BEGIN {
	if ($^O =~ /MSWin/) {require Win32::Console::ANSI; Win32::Console::ANSI->import();} 
}
our %common;
our %config;
our @loads;
our %execute;

our @ISA = qw(Exporter);
our @EXPORT = qw(%common %config %execute @loads extractConfigs checkHash getLogFPathForMail getLogFPath MailFilter setErrSubject setupLogging setupStarting setupConfigMerge getOptions sendSuccessMail sendGeneralMail);

# for command line option parsing, these are merged into %config, %common and @loads
my @optload; my %opt;
my @coreConfig = ("DB","File","FTP","process");
my @extConfig = (@coreConfig,"config");
my %hashCheck = (
	common => {
		process => {},
		DB => {},
		FTP => {},
		File => {},
	},
	config => {
		checkLookup => {test => {errmailaddress => "",errmailsubject => "",timeToCheck =>, freqToCheck => "", logFileToCheck => "", logcheck => "",logRootPath =>""},},
		errmailaddress => "",
		errmailsubject => "",
		fromaddress => "",
		folderEnvironmentMapping => {Test => "Test", Dev => "Dev", "" => "Prod"},
		logCheckHoliday => "",
		logs_to_be_ignored_in_nonprod => '',
		sensitive => {}, # sensitive information, may also be placed outside of site.config
		smtpServer => "",
		smtpAuth => {user => '', pwd => ''},
		smtpTimeout => 60,
		testerrmailaddress => '',
		process => {},
		DB => {},
		FTP => {},
		File => {},
	},
	execute=> {
		additionalLookupData => {},
		alreadyMovedOrDeleted => {},
		archivefilenames => [],
		dbh => {},
		env => "", # Prod, Test, Dev, whatever
		envraw => "", # Production has a special significance here as being the empty string (used for paths). Otherwise like env.
		errmailaddress => "", # for central logcheck/errmail sending
		errmailsubject => "", # for central logcheck/errmail sending
		filesProcessed => {},
		filesToArchive => [],
		filesToDelete => [],
		filesToMoveinHistory => [],
		filenames => [],
		filesToRetrieve => [],
		filesToRemove => [],
		filesToWrite => [],
		firstRunSuccess => 1,
		freqToCheck => "", # for logchecker:  frequency to check entries (B,D,M,M1) ...
		homedir => "",
		logFileToCheck => "", # for logchecker: Logfile to be searched
		logcheck => "", # for logchecker: the Logcheck (regex)
		nextStartTime => "",
		processEnd => 1,
		processFail => 1,
		redoFiles => {},
		removeFiles => {},
		retrievedFiles => {},
		retryBecauseOfError => 1, # retryBecauseOfError shows, if a rerun occurs due to errors (for successMail)
		retrySeconds => 1,
		scriptname => "",
		timeToCheck => "", # for logchecker: scheduled time of job (don't look earlier for log entries)
	},
	process => {
		cutOffExt => "",
		data => [], # loaded data: array (rows) of hash refs (columns)
		hadDBErrors => 1,
		historyFolder => "",
		ignoreNoTest => 0,
		logFolder => "",
		logRootPath => "",
		plannedUntil => "2359",
		postDumpProcessing => "", # 
		postReadProcessing => "",
		redoDir => "", # folder where files for redo are contained
		redoFile => 1, # flag for specifying a redo
		retrySecondsErr => 1,
		retrySecondsPlanned => 300,
		skipHolidays => 0,
		skipHolidaysDefault => "AT",
		skipWeekends => 0,
		skipForFirstBusinessDate => 0,
		subtypeconf => "",
		typeconf => "",
	},
	DB => {
		addID => {},
		additionalLookup => "",
		additionalLookupKey => "",
		columnnames => [],
		cutoffYr2000 => 60,
		database => "",
		debugKeyIndicator => "",
		deleteBeforeInsertSelector => "",
		dontWarnOnNotExistingFields => 0,
		doUpdateBeforeInsert => 1,
		DSNTrusted => '',
		DSNUntrusted => '',
		ignoreDuplicateErrs => 1,
		incrementalStore => 1,
		isTrusted => 1,
		keepContent => 1,
		keyfield => "",
		longreadlen => 1024,
		noDBTransaction => 1,
		noDumpIntoDB => 1,
		postDumpExecs => [{execs => ['',''], condition => ''},],
		primkey => "",
		query => "",
		schemaName => "",
		server => {Prod => "", Test => ""},
		tablename => "",
		upsert => 1,
		useKeyForDeleteBeforeInsert => 1,
		updateIfInsertFails => 1,
		db => {user => "", pwd => ""},
	},
	FTP => {
		archiveFolder => "", # folder for archived files on the FTP server
		dontMoveTempImmediately => 1, # if 0 oder missing: rename/move files immediately after writing to FTP to the final name, otherwise (1) a call to moveTempFiles is required to do that
		dontDoSetStat => 1, # no setting of time stamp of remote file to that of local file (avoid error messages of FTP Server if it doesn't support this)
		dontDoUtime => 1, # don't set time stamp of local file to that of remote file
		dontUseQuoteSystemForPwd => 0,
		dontUseTempFile => 1, # directly upload files, without temp files
		fileToArchive => 1,
		fileToRemove => 1,
		FTPdebugLevel => 0, # debug ftp: 0 or ~(1|2|4|8|16|1024|2048), loglevel automatically set to debug for module FTP
		hostkey => "",
		localDir => "",
		maxConnectionTries => 5,
		onlyArchive => 0,
		onlyDoFiletransferToLocalDir => 1,
		path => "", # relative FTP path (under remoteDir), where the file is to be found
		plinkInstallationPath => "",
		port => 22, # ftp/sftp port (leer lassen für default ports 21 (ftp) oder 22 (sftp)...): 5022
		prefix => "ftp", # key for pwd and user in config{FTP}
		privKey => "", # sftp key file location
		queue_size => 1,
		removeFiles => {removeFolders => [], day=>, mon=>, year=>1},
		remoteDir => "", # remote root folder for up-/download: "out/Marktdaten/", path is added then for each filename (load)
		remoteFiles => {},
		remoteHost => {Prod => "", Test => ""}, # ref to hash of IP-addresses/DNS of host(s).
		remove => {removeFolders => ["",""], day=>, mon=>, year=>1}, # for removing archived files on FTP hosts, removeFolders are the folders to be cleaned, day/mon/year is the days/months/years cutoff age for the removed files
		simulate => 0, # only simulate (1) or do actually (0)?
		type => "", # (A)scii or (B)inary
		user => "", # set user directly
		ftpPre => {user => "", pwd => ""}, # set user/pwd for prefix ftpPre
	},
	File => {
		addtlProcessingTrigger => "",
		addtlProcessing => "",
		avoidRenameForRedo => 1,
		columns => {}, # for writeText: Hash of data fields, that are to be written (in order of keys)
		columnskip => {}, # for writeText: boolean hash of column names that should be skipped when writing the file ({column1ToSkip => 1, column2ToSkip => 1, ...})
		customFile => "",
		dontKeepHistory => 1,
		emptyOK => 0,
		encoding => "",
		extract => 1,
		extension => "",
		filename => "",
		firstLineProc => '',
		format_allowLinefeedInData => 1,
		format_beforeHeader => "",
		format_dateColumns => [],
		format_headerColumns => [],
		format_header => "",
		format_eol => "", # for quoted csv specify special eol character (allowing newlines in values)
		format_fieldXpath => {},
		format_namespaces => {},
		format_padding => {},
		format_poslen => [],
		format_quotedcsv => 1, # special parsing/writing of quoted csv data using Text::CSV
		format_sep => "",
		format_sepHead => "",
		format_skip => 2,
		format_stopOnEmptyValueColumn => 1,
		format_suppressHeader => 1,
		format_targetheader => "",
		format_worksheetID => 1,
		format_worksheet => "",
		format_xlformat => "xlsx",
		format_xpathRecordLevel => [],
		format_XML => 1,
		lineCode => "",
		locale => "",
		localFilesystemPath => "",
		optional => 1,
	},
	load => {
		process => {}, # general processing configs
		DB => {}, # DB specific configs
		FTP => {}, # FTP specific configs
		File => {}, # File specific configs
	}
);

# extract config hashes (DB,FTP,File,process) from $arg hash and return as list of hashes. Required config hashes are given in string list @required
sub extractConfigs {
	my $logger = get_logger();
	my ($arg,@required) = @_;
	my @ret;
	if (ref($arg) eq "HASH") {
		for my $req (@required) {
			push(@ret, \%{$arg->{$req}});
			checkHash($ret[$#ret],$req);
		}
	} else {
		my $errStr = "no ref to hash passed when calling ".(caller(1))[3].", line ".(caller(1))[2]." in ".(caller(1))[1];
		$logger->error($errStr) if $logger;
		warn $errStr if !$logger;
	}
	return @ret;
}

# check config hash for validity against hashCheck (valid key entries are there + their valid value types (examples))
sub checkHash {
	my $logger = get_logger();
	my ($hash, $hashName) = @_;
	my $locStr =  " when calling ".(caller(2))[3].", line ".(caller(2))[2]." in ".(caller(2))[1];
	for my $defkey (keys %{$hash}) {
		my $errStr;
		if (!exists($hashCheck{$hashName}{$defkey})) {
			$errStr = "key name not allowed: \$".$hashName."{".$defkey."},";
		} else {
			$errStr = "wrong reference type for value: \$".$hashName."{".$defkey."}" if ref($hashCheck{$hashName}{$defkey}) ne ref($hash->{$defkey});
			$errStr = "wrong type for value: \$".$hashName."{".$defkey."} when calling " if looks_like_number($hashCheck{$hashName}{$defkey}) ne looks_like_number($hash->{$defkey});
		}
		if ($errStr) {
			$logger->error($errStr.$locStr) if $logger;
			warn $errStr.$locStr if !$logger;
		}
	}
}

# path of logfile and path of yesterdays logfile (after rolling) - getLogFPathForMail and getLogFPath can be used in site-wide log.config
our ($LogFPath, $LogFPathDayBefore);
sub getLogFPathForMail {
	return 'file://'.$LogFPath.', or '.'file://'.$LogFPathDayBefore;
};
sub getLogFPath {
	return $LogFPath;
};

# MailFilter is used for filtering error logs (called in log.config) if error was already sent by mail (otherwise floods)...
my $alreadySent = 0;
sub MailFilter {
	my %p = @_;
	return (!$alreadySent and ($p{log4p_level} eq "ERROR" or $p{log4p_level} eq "FATAL") ? $alreadySent = 1 : 0);
};

# sets the error subject for the subsequent error mails from logger->error()
sub setErrSubject {
	my $context = shift;
	Log::Log4perl->appenders()->{"MAIL"}->{"appender"}->{"subject"} = [($execute{envraw} ? $execute{envraw}.": " : "").$config{errmailsubject}.", $context"];
}

# setup logging for Log4perl
sub setupLogging {
	my $arg = shift;
	my $logger = get_logger();
	my ($process) = extractConfigs($arg,"process");
	my $caller = $execute{scriptname};
	my $envpathExtend = ($execute{envraw} ? "/".$execute{envraw}."/" : "");
	my $logFolder = $process->{logRootPath}.$envpathExtend;
	# if logFolder doesn't exist, die with error message.
	die "can't log to logfolder $logFolder (set with \$process{logRootPath}+environment (".$process->{logRootPath}.$envpathExtend."), as it doesn't exist !" if (! -e $logFolder);
	$LogFPath = $logFolder.$caller.".log";
	$LogFPathDayBefore = $logFolder.get_curdate().".".$caller.".log"; # if mail is watched next day, show the rolled file here
	my $logConfig = $ENV{ETL_WRAP_CONFIG_PATH}."/".$execute{envraw}."/log.config"; # environment dependent log config, Prod is in ETL_WRAP_CONFIG_PATH
	die "no log.config existing in $logConfig" if  (! -e $logConfig);
	Log::Log4perl::init($logConfig);
	MIME::Lite->send('smtp', $config{smtpServer}, AuthUser=>$config{sensitive}{smtpAuth}{user}, AuthPass=>$config{sensitive}{smtpAuth}{pwd}, Timeout=>$config{smtpTimeout}) if $config{smtpServer}; # configure err mail sending

	# get email from central log error handling $config{checkLookup}{<>};
	$execute{errmailaddress} = $config{checkLookup}{$execute{scriptname}}{errmailaddress}; # errmailaddress for the process script
	$execute{errmailsubject} = $config{checkLookup}{$execute{scriptname}}{errmailsubject}; # errmailsubject for the process script
	$execute{errmailaddress} = $config{testerrmailaddress} if $execute{envraw};
	if ($execute{errmailaddress}) {
		setErrSubject(""); # no context 
		Log::Log4perl->appenders()->{"MAIL"}->{"appender"}->{"to"} = [$execute{errmailaddress}];
	} else {
		# Production: no errmailaddress found, error message to Testerrmailaddress
		Log::Log4perl->appenders()->{"MAIL"}->{"appender"}->{"to"} = [$config{testerrmailaddress}];
		$logger->error("no errmailaddress found for ".$execute{scriptname});
	}
}

# setup starting conditions and exit if met
sub setupStarting {
	my $arg = shift;
	my $logger = get_logger();
	my ($process) = extractConfigs($arg,"process");
	# if the historyFolder is not a folder in the processing directory build environment-path separately (processing directory is already in its own environment)
	if ($process->{historyFolder} =~ /(.*)[\\|\/](.*?)$/) {
		my ($historyPath,$historyFolder) = ($process->{historyFolder} =~ /(.*)[\\|\/](.*?)$/); # both slash as well as backslash act as path separator
		$process->{historyFolder} = $historyPath.($execute{envraw} ? "\\".$execute{envraw} : "")."\\".$historyFolder;
	}

	my $curdate = get_curdate();
	# skipHolidays is either a calendar or 1 (then defaults to "AT")
	my $holidayCal = $process->{skipHolidays} if $process->{skipHolidays};
	# skipForFirstBusinessDate für "Warten auf ersten Geschäftstag", entweder Kalender oder 1 (then skipHolidaysDefault), kann nicht gemeinsam mit skipHolidays auftreten
	$holidayCal = $process->{skipForFirstBusinessDate} if $process->{skipForFirstBusinessDate};
	# default legacy setting (nur => 1 .. AT Kalender)
	$holidayCal = $process->{skipHolidaysDefault} if ($process->{skipForFirstBusinessDate} == 1 or $process->{skipHolidays} == 1);
	if ($holidayCal) {
		if (is_holiday($holidayCal,$curdate) and !$process->{redoFile}) {
			$logger->info("skip processing (skipHolidays = ".$process->{skipHolidays}.", skipForFirstBusinessDate = ".$process->{skipForFirstBusinessDate}.") as $curdate holiday in $holidayCal !");
			exit 0;
		}
	}
	if (($process->{skipWeekends} or $process->{skipForFirstBusinessDate}) and is_weekend($curdate) and !$process->{redoFile}) {
		$logger->info("skip processing (skipWeekends = ".$process->{skipWeekends}.", skipForFirstBusinessDate = ".$process->{skipForFirstBusinessDate}.") as $curdate is day of weekend !");
		exit 0;
	}
	# wenn irgendwelche Geschäftstage (Nichtgeschäftstage weniger als Kalendertage) seit dem ersten des Monats waren, dann übergehen, wenn $process->{skipForFirstBusinessDate}
	# Nichtgeschäftstage seit dem ersten des Monats zählen:
	if ($process->{skipForFirstBusinessDate} and !$process->{redoFile}) {
		my $nonBusinessDays; my $daysfrom1st = substr($curdate,-2)-1;
		for (1..$daysfrom1st) {
			$nonBusinessDays += (is_weekend(subtractDays($curdate,$_)) or is_holiday($holidayCal,subtractDays($curdate,$_)));
		}
		if ($nonBusinessDays < $daysfrom1st) {
			$logger->info("skip processing (skipForFirstBusinessDate = ".$process->{skipForFirstBusinessDate}.") as processing already took place on a business day before $curdate!");
			exit 0;
		}
	}
	# if notest file exists, then exit here if not set to ignore that...
	if (!$process->{ignoreNoTest} && -e "notest") {
		$logger->info("skip processing, because notest set (file exists).");
		exit 0;
	}
}

# setupConfigMerge creates cascading inheritance of DB/File/FTP settings: %config -> %common -> $loads[] <- options from command line (latter takes more precedence)
sub setupConfigMerge {
	for (@coreConfig) {
		# fill missing keys in order to avoid Can't use an undefined value as a HASH reference errors in hash config merging later
		$common{$_} = {} if !defined($common{$_});
		$config{$_} = {} if !defined($config{$_});
	}
	# merge cmdline option overrides into toplevel global config (DB, FTP, File and process overrides are merged directly into common below)...
	%config=(%config,%{$opt{config}});
	# merge DB/FTP/File global config and cmdline options into common...
	%common=(%common,DB=>{%{$config{DB}},%{$common{DB}},%{$opt{DB}}},FTP=>{%{$config{FTP}},%{$common{FTP}},%{$opt{FTP}}},File=>{%{$config{File}},%{$common{File}},%{$opt{File}}},process=>{%{$config{process}},%{$common{process}},%{$opt{process}}});
	for my $i (0..$#loads) {
		# fill missing keys in order to avoid Can't use an undefined value as a HASH reference errors in hash config merging later
		for (@coreConfig) {
			$loads[$i]{$_} = {} if !defined($loads[$i]{$_});
		}
		# merge common and cmdline option overrides into loads...
		$loads[$i]={%{$loads[$i]},DB=>{%{$common{DB}}, %{$loads[$i]{DB}},%{$optload[$i]{DB}}},FTP=>{%{$common{FTP}},%{$loads[$i]{FTP}},%{$optload[$i]{FTP}}},File=>{%{$common{File}},%{$loads[$i]{File}},%{$optload[$i]{File}}},process=>{%{$common{process}},%{$loads[$i]{process}},%{$optload[$i]{process}}}};
	}
}

# get options for overriding configured settings
sub getOptions() {
	# construct option definitions for Getopt::Long::GetOptions, everything parsed as a string first
	my %optiondefs = ("DB=s%" => \$opt{DB}, "FTP=s%" => \$opt{FTP}, "File=s%" => \$opt{File},"process=s%" => \$opt{process}, "config=s%" => \$opt{config});
	for my $i (0..$#loads) {
		 %optiondefs = (%optiondefs, "load${i}DB=s%" => \$optload[$i]{DB}, "load${i}FTP=s%" => \$optload[$i]{FTP}, "load${i}File=s%" => \$optload[$i]{File},"load${i}process=s%" => \$optload[$i]{process});
	}
	Getopt::Long::GetOptions(%optiondefs);
	# now correct strings to numeric where needed, also checking validity
	for my $hashName (@extConfig) {
		for my $defkey (keys %{$opt{$hashName}}) {
			my $errStr;
			if (!exists($hashCheck{$hashName}{$defkey})) {
				die("option not allowed: --$hashName $defkey=<value>");
			} else {
				$opt{$hashName}{$defkey} = 0+$opt{$hashName}{$defkey} if looks_like_number($hashCheck{$hashName}{$defkey});
			}
		}
		for my $i (0..$#loads) {
			next if $hashName eq "config"; # no config in loads...
			for my $defkey (keys %{$optload[$i]{$hashName}}) {
				my $errStr;
				if (!exists($hashCheck{$hashName}{$defkey})) {
					die("option not allowed: --load$i$hashName $defkey=<value>");
				} else {
					$optload[$i]{$hashName}{$defkey} = 0+$optload[$i]{$hashName}{$defkey} if looks_like_number($hashCheck{$hashName}{$defkey});
				}
			}
		}
	}
}

# this is used for ETL::Wrap::processingEnd to notify of successful retry after failure
sub sendSuccessMail {
	my $filename = shift;
	my $logger = get_logger();
	sendGeneralMail("", ($execute{envraw} ? $config{testerrmailaddress} : $config{errmailaddress}), "", "",
	'Successful retry of '.$execute{scriptname}.' !'.($execute{envraw} ? '('.$execute{envraw}.')' : ""),'TEXT',$filename.' was succesfully done on retry.');
}

# general mail sending for notifying of conditions/sending reports (in user specific code)
sub sendGeneralMail {
	my ($From, $To, $Cc, $Bcc, $Subject, $Type, $Data, $Encoding, $AttachType, $AttachFile) = @_;
	my $logger = get_logger();
	$logger->info("sending general mail From:$From, To:$To, CC:$Cc, Bcc:$Bcc, Subject:$Subject, Type:$Type, Encoding: $Encoding, AttachType:$AttachType, AttachFile:$AttachFile ...");
	$logger->debug("Mailbody: $Data");
	my $msg = MIME::Lite->new(
			From    => ($From ? $From : $config{fromaddress}),
			To      => ($execute{envraw} ? $config{testerrmailaddress} : $To),
			Cc      => ($execute{envraw} ? "" : $Cc),
			Bcc     => ($execute{envraw} ? "" : $Bcc),
			Subject => ($execute{envraw} ? $execute{envraw}.": " : "").$Subject,
			Type    => $Type,
			Data    => ($Type eq 'multipart/related' ? undef : $Data),
			Encoding => ($Type eq 'multipart/related' ? undef : $Encoding)
		);
	$logger->error("couldn't create msg  for mail sending..") unless $msg;
	if ($Type eq 'multipart/related') {
		$msg->attach(
			Type => 'text/html',
			Data    => $Data,
			Encoding => $Encoding
		);
		for (@$AttachFile) {
			$msg->attach(
				Encoding => 'base64',
				Type     => $AttachType,
				Path     => $_,
				Id       => $_,
			);
		}
	} elsif ($AttachFile and $AttachType) {
		$msg->attach(
			Type => $AttachType,
			Id   => $AttachFile,
			Path => $AttachFile
		);
	}
	$msg->send('smtp', $config{smtpServer}, AuthUser=>$config{smtpAuth}{user}, AuthPass=>$config{smtpAuth}{pwd});
	if ($msg->last_send_successful()) {
		$logger->info("Mail sent");
		$logger->trace("sent message: ".$msg->as_string) if $logger->is_trace();
	}
}
1;
__END__

=head1 NAME

ETL::Wrap::Common - Common parts for the ETL::Wrap package

=head1 SYNOPSIS

 %common .. common load configs set process script
 %config .. hash for global config (typically set in site.config)
 @loads .. list of hashes defining load processes
 %execute .. hash of parameters for current process (having one or multiple loads)

 getLogFPathForMail
 getLogFPath
 MailFilter
 setupLogging $process
 setErrSubject $context
 setupStarting $process
 setupConfigMerge
 getOptions
 sendSuccessMail $filename
 sendGeneralMail $From, $To, $Cc, $Bcc, $Subject, $Type, $Data, $Encoding, $AttachType, $AttachFile

=head1 DESCRIPTION

=item getLogFPathForMail, getLogFPath, MailFilter: funktions that are used in the central log.config as coderef.
=item getLogFPathForMail: for custom conversion specifiers: path of logfiles resp logfiles of previous day (as file: hyperlink)
=item getLogFPath: for file appender config, path of current logfile.
=item MailFilter: for Mail appender config: used for filtering if mail should be sent? contains throttling "alreadySent" for mass errors
=item setupLogging: set up logging from process config information and central log.config
 $process .. config information

=item setErrSubject: set context specific subject for ErrorMail
 $context .. text for context of subject

=item setupStarting: setup starting conditions from process config information and exit if met
 $process .. config information

=item setupConfigMerge: creates cascading inheritance of DB/File/FTP settings: %config -> %common -> $loads[] <- options from command line (latter takes more precedence)
=item getOptions: get options for overriding configured settings
=item sendSuccessMail: send mail to configured errmailadress when a repeated processing is successful
 $filename .. file being processed successfully

=item sendGeneralMail: send general mail, either simple text or html mails, mails with an attachment or multipart mails for "in-body" attachments (eg pictures). 
 In this case the mail body needs to be HTML, attachments are referred to inside the HTML code and are passed as a ref to array of paths in $AttachFile.
 
 Example:
 # prepare body with refererring to attachments in a HTML table
 my $body='<style>table, th, td {border: 1px solid black;border-collapse: collapse;} th, td {padding: 5px;text-align: right;}}</style>';
 $body.='<table style="width:1600px; border:0; text-align:center;" cellpadding="0" cellspacing="0">
 <tr><td width="800px" height="800px"><img style="display:block;"  width="100%" height="100%" src="cid:relativePathToPic.png" alt="alternateDescriptionOfPic"/></td></tr>';
 # pass needed files to sendGeneralMail:
 my @filelist = glob("*.png");
 # call sendGeneralMail with prepared parmaters
 sendGeneralMail(undef,'address@somewhere.com',undef,undef,"subject for mail",'multipart/related',$body,'quoted-printable','image/png',\@filelist);
 $From .. sender
 $To .. recipient
 $Cc .. cc recipient (optional, but need arg)
 $Bcc .. mcc recipient  (optional, but need arg)
 $Subject .. mail subject
 $Type .. mail mime type (eg text/plain, text/html oder 'multipart/related'), if 'multipart/related', then a ref to array to the filenames (path), that should be attached is expected to be set in $AttachFile. 
 In the above example a mail body ($Data) is being set as the first attachment and its type is text/html. The rest of the attachments from $AttachFile are encoded using base64 and all have mime type AttachType (see below).
 $Data .. the mail body, either plain text or html.
 $Encoding .. encoding for mail body, optional (eg quoted-printable)
 $AttachType .. mime type for attachment(s) (eg text/csv or image/png), optional
 $AttachFile .. file name/path(s) for attachment(s), optional (hat to be ref to array, if $Type = 'multipart/related')

=head1 COPYRIGHT

Copyright (c) 2022 Roland Kapl

All rights reserved.  This program is free software; you can
redistribute it and/or modify it under the same terms as Perl itself.

The full text of the license can be found in the LICENSE file included
with this module.

=cut