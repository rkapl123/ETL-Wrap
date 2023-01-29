use strict; use warnings; use Log::Log4perl qw(get_logger); use Log::Log4perl::Level; use Test::More; use Data::Dumper;

use ETL::Wrap::FTP; use ETL::Wrap::Common;

LogCfgUtil::setupLogging("UnitTestFTPUtil");
my $logger = get_logger();
get_logger("FTPUtil")->level($TRACE);
$FTPUtil::parm = {
		RemoteHost => { "Prod" => "select.datascope.refinitiv.com", "Test" => "select.datascope.refinitiv.com"},
		RemoteDir => "reports",
		SFTP => 1,
		port => 22,
		prefix => "DSSftp",
		environment => "Test",
};
my $ftp;
FTPUtil::login(\$ftp);
ok($ftp,"login success");
$ftp->quit() if !$FTPUtil::parm->{"SFTP"};

# TODO:
# FTP login
# removeFilesOlderX (FTP und SFTP)
# fetchFiles (FTP und SFTP)
# writeFiles (FTP und SFTP)
# moveTempFiles (FTP und SFTP)
# archiveFiles (FTP und SFTP)
done_testing();