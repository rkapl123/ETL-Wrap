use strict; use warnings; use Log::Log4perl qw(get_logger); use Log::Log4perl::Level; use Test::More; use Data::Dumper;
use Test::More tests => 2;
use Test::Files;
use File::Spec;

my $existing_file  = File::Spec->catfile("Test.txt");
my $existing_file2  = File::Spec->catfile("Test2.txt");

use ETL::Wrap::File; use ETL::Wrap::Common;

our %theData;
use LogCfgUtil;
LogCfgUtil::setupLogging("UnitTestFS");
my $logger = get_logger();
$logger->level($TRACE);
get_logger("FS")->level($TRACE);

$theData{Test} = {
	encoding => ":encoding(utf8)",
	filename => "Test.txt",
	format => {
		autoheader => 1,
		sep => "\t",
	},
};
$theData{Testout} = {
	sep => "\t",
	format => "sep",
	filename => "Testout.txt",
	columns => ["Testcol1","Testcol2","Testcol3","Testcol4"],
};

$theData{Testout2} = {
	sep => ",",
	sepHead => "\n",
	format => "sep",
	beforeHeader => "Vor allen Headern\n",
	filename => "Testout2.txt",
	columns => ["TestcolIgnoreA","Testcol1","Testcol2","TestcolIgnoreB","Testcol3","Testcol4","TestcolIgnoreC"],
	columnskip => {"TestcolIgnoreA" => 1,"TestcolIgnoreB" => 1,"TestcolIgnoreC" => 1},
};


FS::readFile($theData{Test},"Test","");
$theData{Testout}{data} = $theData{Test}{source};
$theData{Testout2}{data} = $theData{Test}{source};
FS::writeFile($theData{Testout},"Testout");
FS::writeFile($theData{Testout2},"Testout2");
my $created_file = File::Spec->catfile("Testout.txt");
my $created_file2 = File::Spec->catfile("Testout2.txt");
compare_ok($existing_file, $created_file, "Test und Testout sollten gleich sein!");
compare_ok($existing_file2, $created_file2, "Test2 und Testout2 sollten gleich sein!");


# TODO:
# readFile mit $redoSubDir

done_testing();