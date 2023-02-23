use strict; use warnings; use Log::Log4perl qw(get_logger); use Log::Log4perl::Level; use Test::More; use Data::Dumper;
use Test::More tests => 6; use Test::Files; use File::Spec;
use ETL::Wrap::File; use ETL::Wrap::Common;

Log::Log4perl::init("testlog.config");
my $logger = get_logger();

my ($expected_filecontent,$expected_datastruct,$File,$process);

# write data to tab separated file
$expected_filecontent = "col1\tcol2\tcol3\nval11\tval21\tval31\nval12\tval22\tval32\n";
$File = {format => {sep => "\t"},filename => "Testout.txt",columns => {1=>"col1",2=>"col2",3=>"col3"},};
$process = {data => [{col1 => "val11",col2 => "val21",col3 => "val31"},{col1 => "val12",col2 => "val22",col3 => "val32"}]};
writeText($File,$process);
file_ok("Testout.txt",$expected_filecontent,"Testout.txt should be written content");

# write data to csv file including quotes in header and values
$expected_filecontent = "\"col 1\",col2,col3\n\"val 11\",val21,val31\n\"val 12\",val22,val32\n";
$File = {format => {sep => ",", quotedcsv => 1},filename => "Testout.csv",columns => {1=>"col 1",2=>"col2",3=>"col3"},};
$process = {data => [{"col 1" => "val 11",col2 => "val21",col3 => "val31"},{"col 1" => "val 12",col2 => "val22",col3 => "val32"}]};
writeText($File,$process);
file_ok("Testout.csv",$expected_filecontent,"Testout.csv should be written content");

# write data to excel file
$expected_filecontent = "";
$File = {format => {xlformat => "xlsx"},filename => "Testout.xlsx",columns => {1=>"col1",2=>"col2",3=>"col3"},};
$process = {data => [{col1 => "val11",col2 => "val21",col3 => "val31"},{col1 => "val12",col2 => "val22",col3 => "val32"}]};
#writeExcel($File,$process);
dir_contains_ok(".",["Testout.xlsx"],"Testout.xlsx was written");

# read data from tab separated file
$File =  {format => {skip => 1, sep => "\t",header => "col1\tcol2\tcol3",targetheader => "col1\tcol2\tcol3",},filename => "Testout.txt",};
$process = {data => []}; # need to init process structure with data key
$expected_datastruct = [{col1 => "val11",col2 => "val21",col3 => "val31"},{col1 => "val12",col2 => "val22",col3 => "val32"}];
readText($File,$process,["Testout.txt"]);
is_deeply($process->{data},$expected_datastruct,"read in tab sep data should be expected content");

# read csv data from file including quotes in header and values
$File =  {format => {skip => 1, sep => ",", quotedcsv => 1, header => "col 1,col2,col3", targetheader => "col 1,col2,col3",},filename => "Testout.csv",};
$process = {data => []}; # need to init process structure with data key
$expected_datastruct = [{"col 1" => "val 11",col2 => "val21",col3 => "val31"},{"col 1" => "val 12",col2 => "val22",col3 => "val32"}];
readText($File,$process,["Testout.csv"]);
is_deeply($process->{data},$expected_datastruct,"read in csv data should be expected content");

# read data from excel file
$File =  {format => {xlformat => "xlsx", skip => 1, worksheetID=>1, header => "col1\tcol2\tcol3",targetheader => "col1\tcol2\tcol3",},filename => "Testout.xlsx",};
$process = {data => []}; # need to init process structure with data key
$expected_datastruct = [{col1 => "val11",col2 => "val21",col3 => "val31"},{col1 => "val12",col2 => "val22",col3 => "val32"}];
readExcel($File,$process,["Testout.xlsx"]);
is_deeply($process->{data},$expected_datastruct,"read in excel data should be expected content");

unlink "Testout.txt";
unlink "Testout.csv";
#unlink "Testout.xlsx";

# TODO:
# readFile with $redoSubDir

done_testing();