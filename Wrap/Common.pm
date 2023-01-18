package ETL::Wrap::Common;

use strict;
use Exporter; use Log::Log4perl qw(get_logger); use ETL::Wrap::DateUtil; use Data::Dumper; use Getopt::Long qw(:config no_ignore_case); use Scalar::Util qw(looks_like_number);
# to make use of colored logs with Log::Log4perl::Appender::ScreenColoredLevels on windows we have to use that (special "use" to make this optional on non-win environments)
BEGIN {
	if ($^O =~ /Win(?:32|64)/) {require Win32::Console::ANSI; Win32::Console::ANSI->import();} 
}
our %common;
our %config;
our @loads;
our %execute;

our @ISA = qw(Exporter);
our @EXPORT = qw(%common %config %execute @loads extractConfigs checkHash getLogFPathForMail getLogFPath MailFilter setErrSubject setupLogging setupStarting setupConfigMerge getOptions sendSuccessMail sendGeneralMail);

# for commandline option parsing, these are merged into %config, %common and @loads
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
		checkLookup => {test => {errmailaddress => "",errmailsubject => "",timeToCheck =>, freqToCheck => "", logFileToCheck => "", logcheck => ""},},
		errmailaddress => "",
		errmailsubject => "",
		fromaddress => "",
		folderEnvironmentMapping => {Test => "Test", Dev => "Dev", "" => "Prod"},
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
		db => {user => "", pwd => ""},
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
	},
	FTP => {
		archiveFolder => "", # Ordner für archivierte files auf dem FTP server: "Archiv"
		dontMoveTempImmediately => 1, # wenn 0 oder fehlt: files sofort (im Anschluss ans Schreiben auf den FTP) final umbenennen, ansonsten wird zur finalen Umbenennung der Aufruf von moveTempFiles benötigt.
		dontDoSetStat => 1, # kein Setzen der Berechtigungen bzw. des Timestamps (zum Vermeiden von Fehlermeldungen bei manchen FTP Servern)
		dontDoUtime => 1, # don't set timestamp of local file to that of remote file
		dontUseQuoteSystemForPwd => 0,
		dontUseTempFile => 1, # direktes hinaufladen der Files, sonst vorgehen mit temp files
		filenameToArchive => 1,
		filenameToRemove => 1,
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
		user => "",
		ftp => {user => "", pwd => ""},
	},
	File => {
		additionalColAction => "",
		additionalColTrigger => "",
		addtlProcessingTrigger => "",
		addtlProcessing => "",
		avoidRenameForRedo => 1,
		beforeHeader => "",
		columns => {}, # columns: Hash of data fields, that are to be written (in order of keys)
		columnskip => {},
		customFile => "",
		dontKeepHistory => 1,
		emptyOK => 0,
		encoding => "",
		extract => 1,
		extension => "",
		fieldCode => "",
		fieldCodeSpec => "",
		filename => "",
		firstLineProc => '',
		format => {
			header => "",
			sep => "",
			sepHead => "",
			skip => 2,
			xls => 1,
			xlsx => 1,
			XML => 1,
		},
		headings => [],
		lineCode => "",
		locale => "",
		localFilesystemPath => "",
		optional => 1,
		padding => {},
		suppressHeader => 1,
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
	my ($hash, $hashName, $debugme) = @_;
	my $locStr =  " when calling ".(caller(2))[3].", line ".(caller(2))[2]." in ".(caller(2))[1];
	for my $defkey (keys %{$hash}) {
		my $errStr;
		if (!exists($hashCheck{$hashName}{$defkey})) {
			$errStr = "not allowed: \$".$hashName."{".$defkey."},";
		} else {
			$errStr = "wrong reference type for: \$".$hashName."{".$defkey."}" if ref($hashCheck{$hashName}{$defkey}) ne ref($hash->{$defkey});
			$errStr = "wrong type for: \$".$hashName."{".$defkey."} when calling " if looks_like_number($hashCheck{$hashName}{$defkey}) ne looks_like_number($hash->{$defkey});
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
	die "no log.config existing in ".$ENV{ETL_WRAP_CONFIG_PATH}."/".$execute{envraw}."/log.config" if  (! -e $ENV{ETL_WRAP_CONFIG_PATH}."/".$execute{envraw}."/log.config");
	Log::Log4perl::init($ENV{ETL_WRAP_CONFIG_PATH}."/".$execute{envraw}."/log.config"); # environment dependent log config, Prod is in ETL_WRAP_CONFIG_PATH
	MIME::Lite->send('smtp', $config{smtpServer}, AuthUser=>$config{smtpAuth}{user}, AuthPass=>$config{smtpAuth}{pwd}, Timeout=>$config{smtpTimeout}); # configure err mail sending

	# central log error handling from $config{checkLookup}{<>};
	$execute{errmailaddress} = $config{checkLookup}{$execute{scriptname}}{errmailaddress}; # errmailaddress for the process script
	$execute{errmailsubject} = $config{checkLookup}{$execute{scriptname}}{errmailsubject}; # errmailsubject for the process script
	#$execute{timeToCheck} = $config{checkLookup}{$execute{scriptname}}{timeToCheck}; # scheduled time of job (don't look earlier for log entries)
	#$execute{freqToCheck} = $config{checkLookup}{$execute{scriptname}}{freqToCheck}; # frequency to check entries (B,D,M,M1) ...
	#$execute{logFileToCheck} = $config{checkLookup}{$execute{scriptname}}{logFileToCheck}; # Logfile to be searched
	#$execute{logcheck} = $config{checkLookup}{$execute{scriptname}}{logcheck}; # Logcheck (regex)
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

 %config .. hash for global config (typically set in site.config)
 %load .. hash of type hashes defining loads within an execution
 %execute .. hash of parameters for current execution (having one or multiple loads)

 getLogFPathForMail
 getLogFPath
 MailFilter
 setupLogging $caller, $configFile
 setErrSubject $context
 setLogLevels 
 setupETLWrap
 sendGeneralMail $To, $Bcc, $Subject, $Type, $Data
 sendSuccessMail $filename

=head1 DESCRIPTION

=item getLogFPathForMail, getLogFPath, MailFilter: Funktionen, die im zentralen log.config als codereferenz verwendet werden.
=item getLogFPathForMail: für custom conversion specifiers: Pfad des logfiles bzw. des logfiles vom Vortag (als hyperlink)
=item getLogFPath: für File appender config, der Pfad des aktuellen logfiles.
=item MailFilter: für Mail appender config: Wann wird ein Mail verschickt? Beinhaltet auch die Drossel "alreadySent" für Massenfehler...
=item setupLogging: set up logging from central log.config
=item setErrSubject: setzt kontextspezifisches Subject für ErrorMail.
 $context .. Text für den Kontext des Subjects

=item readConfigFile: Einlesen, evaluieren des config files und initiale Verarbeitungen.
 $configFileOverride .. Name des configFiles, falls nicht ident mit dem Aufrufer von setupLogging
 
=item setLogLevels: Setze loglevels für alle Module oder bestimmte in dem file mit Namen $loglevel (debug oder trace) ausgewählte Module.
 $loglevel .. loglevel, der gesetzt werden soll ("debug" oder "trace": dazu muss jeweils ein file (namens debug bzw. trace) im Verzeichnis existieren mit folgendem Inhalt (Zeilen): Module, die den debug oder trace level haben sollen); loglevel info wird ohne file gesetzt)

=item sendSuccessMail: Sende Mail an $errmailadress bei erfolgreicher Wiederholungsverarbeitung
 $filename .. File das bei Wiederholung verarbeitet wurde

=item sendGeneralMail: Sende allgemeines Mail, entweder einfache text bzw. html mails, Mails mit einem Attachment oder multipart Mails für "in-body" attachments (zb bilder). 
 In diesem Fall wird angenommen, dass der Mailbody HTML ist, die Attachments darin verwiesen werden und im $AttachFile als ref to array von pfadangaben mitgegeben werden.
 Beispiel:
 my $body='<style>table, th, td {border: 1px solid black;border-collapse: collapse;} th, td {padding: 5px;text-align: right;}}</style>';
 $body.='<table style="width:1600px; border:0; text-align:center;" cellpadding="0" cellspacing="0">
 <tr><td width="800px" height="800px"><img style="display:block;"  width="100%" height="100%" src="cid:ATST01D_Verteilung.png" alt="ATST01D_Verteilung"/></td></tr>';
 # Benötigte Files mitgeben an sendGeneralMail
 my @filelist = glob("*.png");

 LogCfgUtil::sendGeneralMail(undef,'CONTROLLING@oebfa.at',undef,undef,"Zinsszenarien erstellt, anbei Übersicht",'multipart/related',$body,'quoted-printable','image/png',\@filelist);
 $From .. Sender (wenn leer, dann noreply.pl@oebfa.at)
 $To .. Empfänger
 $Cc .. Cc empfänger (optional aber als Argument zu setzen)
 $Bcc .. Bcc empfänger (optional aber als Argument zu setzen)
 $Subject .. Mail subject
 $Type .. Mailtyp (z.b. text/plain, text/html oder 'multipart/related'), wenn 'multipart/related', dann wird in $AttachFile ein ref to array auf die filenamen (pfad), die anzuhängen sind angenommen. 
 Hier wird dann der Mailbody ($Data) als eigenes/erstes Attachment angehängt und dessen Type auf text/html gesetzt. Die restlichen Attachments aus $AttachFile werden mit Encoding base64 und dem gleichen AttachType angehängt.
 $Data .. Mailbody
 $Encoding .. Encoding für Mailbody, optional (zb quoted-printable)
 $AttachType .. Typ für Attachment (zb text/csv oder image/png), optional
 $AttachFile .. Filename/pfad für Attachment, optional (muss ein ref to array sein, wenn $Type = 'multipart/related')

=item setupETLWrap: Einlesen des parameter files für checkLogExist.pl und der zentral gewarteten Mailadressen/Subjects für die Errormails aus dumpFTPFiles.pl/uploadFTPFiles.pl
 $parm->{"CheckParamFile"} .. zentrales parameter file für checkLogExist und auch Mailadressen, an die Errormails geschickt werden sollen
 $logFileToCheck,$freqToCheck,$timeToCheck,$mailAddressToSend,$logcheck .. die lookups für die Verwendung in checkLogExist.pl (Rückgabeparameter)
 $configMailLookup .. lookup der zentralen Mailadressen, an die Fehlermails geschickt werden sollen (Schlüssel: Konfigurationsfile + potentielle subkonfig(s)). Wenn nicht angegeben, dann Name des configFiles (angegeben beim Aufruf von setupLogging)

=head1 COPYRIGHT

Copyright (c) 2022 Roland Kapl

All rights reserved.  This program is free software; you can
redistribute it and/or modify it under the same terms as Perl itself.

The full text of the license can be found in the LICENSE file included
with this module.

=cut