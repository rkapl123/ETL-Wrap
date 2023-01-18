use ETL::Wrap; use Log::Log4perl qw(get_logger);
use Data::Dumper;

%common = (
	FTP => {
		remoteDir => "",
		remoteHost => {Prod => "localhost", Test => "localhost"},
		FTPdebugLevel => 0, #~(1|2|4|8|16|1024|2048) or -1 for everything...
		removeFiles => {removeFolders => ["",""], day=>, mon=>, year=>1},
		maxConnectionTries => 2,
		privKey => "",
		prefix => "ftp",
		dontUseQuoteSystemForPwd => 0,
		archiveFolder => "",
		simulate => 0,
		dontUseTempFile => 1,
		dontMoveTempImmediately => 1,
	},
	DB => {
		database => "ORE",
	},
	process => {
		retrySecondsErr => 30,
		retrySecondsPlanned => 300,
		#plannedUntil => "2359",
		skipHolidays => 0,
		skipHolidaysDefault => "AT",
		skipWeekends => 0,
		skipForFirstBusinessDate => 0,
		ignoreNoTest => 0,
	},
);

@loads = (
	{
		DB => {
			primkey => "Registerkonto = ? AND InhaberNummer = ?",
			keepContent => 1,
			tablename => "BSKunden",
		},
		File => {
			dontKeepHistory => 1,
			filename => "test.txt",
			locale => "german",
			format => {
				sep => "\t",
				skip => 2,
				header => "Registerkonto	InhaberNummer	Name	Kapital",
			},
			firstLineProc => 'my ($repyear,$repmonth,$repday)=/Kundenreport per (\d{4})-(\d{2})-(\d{2})/i; $loads[0]{ReferenceDate} = sprintf("%04d%02d%02d",$repyear,$repmonth,$repday);',
		}
	},
	{
		DB => {
			primkey => "Registerkonto = ? AND InhaberNummer = ?",
			keepContent => 1,
			tablename => "BSKunden",
		},
		File => {
			dontKeepHistory => 1,
			filename => "test.txt",
			locale => "german",
			format => {
				sep => "\t",
				skip => 2,
				header => "Registerkonto	InhaberNummer	Name	Kapital",
			},
			firstLineProc => 'my ($repyear,$repmonth,$repday)=/Kundenreport per (\d{4})-(\d{2})-(\d{2})/i;$loads[0]{ReferenceDate} = sprintf("%04d%02d%02d",$repyear,$repmonth,$repday);',
		}
	},
);
$loads[0]{File}{lineCode} = <<'END';
	$line{"Monatsletzter"} = 0;
	# verbundene Zusatztabelle BSKapitalstand befüllen (tägliche Änderungen des Kapitalstands)
	my %linkedKapital;
	$linkedKapital{"Registerkonto"} = $line{"Registerkonto"};
	$linkedKapital{"InhaberNummer"} = $line{"InhaberNummer"};
	$linkedKapital{"StichDatum"} = $loads[0]{ReferenceDate};
	$linkedKapital{"Kapitalstand"} = $line{"Kapital"};
	push @{$loads[0]{linkedKapital}}, \%linkedKapital;
END

ETL::Wrap::setupETLWrap();
# ETL::Wrap::removeFilesinFolderOlderX(\%common);
ETL::Wrap::openDBConn(\%common) or die;
#ETL::Wrap::openFTPConn(\%common) or die;
while (!$execute{processEnd}) {
	for my $load (@loads) {
		#ETL::Wrap::openDBConn($load);
		ETL::Wrap::openFTPConn($load);
		#ETL::Wrap::getLocalFiles($load);
		ETL::Wrap::getFilesFromFTP($load);
		if (ETL::Wrap::checkFiles($load)) {
			#ETL::Wrap::getAdditionalDBData($load);
			ETL::Wrap::readFileData($load);
			ETL::Wrap::dumpDataIntoDB($load);
			ETL::Wrap::markProcessed($load);
		}
	}
	ETL::Wrap::processingEnd(\%common);
}
