use strict; use warnings; use Log::Log4perl qw(get_logger); use Log::Log4perl::Level; use Test::More; use Data::Dumper;
use ETL::Wrap::FTP ;use Test::Files; use File::Spec;
use Test::More tests => 1;

Log::Log4perl::init("testlog.config");
my $logger = get_logger();

$FTP::parm = {
		RemoteHost => { "Prod" => "select.datascope.refinitiv.com", "Test" => "select.datascope.refinitiv.com"},
		RemoteDir => "reports",
		SFTP => 1,
		port => 22,
		prefix => "DSSftp",
		environment => "Test",
};
my $ftp;
ETL::Wrap::FTP::login();
ok($ETL::Wrap::FTP::ftp,"login success");

# TODO:
# FTP login
# removeFilesOlderX (FTP und SFTP)
# fetchFiles (FTP und SFTP)
# writeFiles (FTP und SFTP)
# moveTempFiles (FTP und SFTP)
# archiveFiles (FTP und SFTP)
done_testing();