use ETL::Wrap; use Data::Dumper;

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
			primkey => "ID1 = ? AND ID2 = ?",
			keepContent => 1,
			tablename => "TestTable",
		},
		File => {
			dontKeepHistory => 1,
			filename => "test.txt",
			locale => "german",
			format_sep => "\t",
			format_skip => 2,
			format_header => "ID1	ID2	Name	Number",
		}
	},
	{
		DB => {
			primkey => "ID1 = ? AND ID2 = ?",
			keepContent => 1,
			tablename => "TestTable",
		},
		File => {
			dontKeepHistory => 1,
			filename => "test.txt",
			locale => "german",
			format_sep => "\t",
			format_skip => 2,
			format_header => "ID1	ID2	Name	Number",
		}
	},
);

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
