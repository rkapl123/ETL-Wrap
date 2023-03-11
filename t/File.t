use strict; use warnings; use Log::Log4perl qw(get_logger); use Log::Log4perl::Level; use Test::More; use Data::Dumper;
use ETL::Wrap::File; use Test::Files; use File::Spec;
use Test::More tests => 14; 

Log::Log4perl::init("testlog.config");
my $logger = get_logger();

is(ETL::Wrap::File::normalizeNumerics("22.123.123,013","\\.","\\,"),"22123123.013",'normalizeNumericsThousandSep');
is(ETL::Wrap::File::normalizeNumerics("123,01E-107","\\.","\\,"),"123.01E-107",'normalizeNumericsScientific');
my ($expected_filecontent,$expected_datastruct,$File,$process);

# write data to tab separated file
$expected_filecontent = "col1\tcol2\tcol3\nval11\tval21\tval31\nval12\tval22\tval32\n";
$File = {format_sep => "\t",filename => "Testout.txt",columns => {1=>"col1",2=>"col2",3=>"col3"},};
$process = {data => [{col1 => "val11",col2 => "val21",col3 => "val31"},{col1 => "val12",col2 => "val22",col3 => "val32"}]};
writeText($File,$process);
file_ok("Testout.txt",$expected_filecontent,"Testout.txt is written content");

# write data to csv file including quotes in header and values
$expected_filecontent = "\"col 1\",col2,col3\n\"val 11\",val21,val31\n\"val 12\",val22,val32\n";
$File = {format_sep => ",", format_quotedcsv => 1,filename => "Testout.csv",columns => {1=>"col 1",2=>"col2",3=>"col3"},};
$process = {data => [{"col 1" => "val 11",col2 => "val21",col3 => "val31"},{"col 1" => "val 12",col2 => "val22",col3 => "val32"}]};
writeText($File,$process);
file_ok("Testout.csv",$expected_filecontent,"Testout.csv is written content");

# write data to excel xlsx file
$expected_filecontent = "";
$File = {format_xlformat => "xlsx",filename => "Testout.xlsx",columns => {1=>"col1",2=>"col2",3=>"col3"},};
$process = {data => [{col1 => "val11",col2 => "val21",col3 => "val31"},{col1 => "val12",col2 => "val22",col3 => "val32"}]};
writeExcel($File,$process);
dir_contains_ok(".",["Testout.xlsx"],"Testout.xlsx was written");

# write data to excel xlsx file
$expected_filecontent = "";
$File = {format_xlformat => "xls", filename => "Testout.xls",columns => {1=>"col1",2=>"col2",3=>"col3"},};
$process = {data => [{col1 => "val11",col2 => "val21",col3 => "val31"},{col1 => "val12",col2 => "val22",col3 => "val32"}]};
writeExcel($File,$process);
dir_contains_ok(".",["Testout.xls"],"Testout.xls was written");

# read data from tab separated file
$File =  {format_skip => 1, format_sep => "\t",format_header => "col1\tcol2\tcol3",format_targetheader => "col1\tcol2\tcol3",filename => "Testout.txt",};
$process = {data => []}; # need to init process structure with data key
$expected_datastruct = [{col1 => "val11",col2 => "val21",col3 => "val31"},{col1 => "val12",col2 => "val22",col3 => "val32"}];
readText($File,$process,["Testout.txt"]);
is_deeply($process->{data},$expected_datastruct,"read in tab sep data is expected content");

# read csv data from file including quotes in header and values
$File =  {format_skip => 1, format_sep => ",", format_quotedcsv => 1, format_header => "col 1,col2,col3", format_targetheader => "col 1,col2,col3",filename => "Testout.csv",};
$process = {data => []}; # need to init process structure with data key
$expected_datastruct = [{"col 1" => "val 11",col2 => "val21",col3 => "val31"},{"col 1" => "val 12",col2 => "val22",col3 => "val32"}];
readText($File,$process,["Testout.csv"]);
is_deeply($process->{data},$expected_datastruct,"read in csv data is expected content");

# read data from excel file
$File =  {format_xlformat => "xlsx", format_skip => 1, format_worksheetID=>1, format_header => "col1\tcol2\tcol3",format_targetheader => "col1\tcol2\tcol3",filename => "Testout.xlsx",};
$process = {data => []}; # need to init process structure with data key
$expected_datastruct = [{col1 => "val11",col2 => "val21",col3 => "val31"},{col1 => "val12",col2 => "val22",col3 => "val32"}];
readExcel($File,$process,["Testout.xlsx"]);
is_deeply($process->{data},$expected_datastruct,"read in excel xlsx data is expected content");

# read data from excel file
$File =  {format_xlformat => "xls", format_skip => 1, format_worksheetID=>1, format_header => "col1\tcol2\tcol3",format_targetheader => "col1\tcol2\tcol3",filename => "Testout.xls",};
$process = {data => []}; # need to init process structure with data key
$expected_datastruct = [{col1 => "val11",col2 => "val21",col3 => "val31"},{col1 => "val12",col2 => "val22",col3 => "val32"}];
readExcel($File,$process,["Testout.xls"]);
is_deeply($process->{data},$expected_datastruct,"read in excel xls data is expected content");

# read data from excel file using format_headerColumns
$File =  {format_xlformat => "xls", format_skip => 1, format_worksheetID=>1, format_headerColumns => [1,3], format_header => "col1\tcol3",format_targetheader => "col1\tcol3",filename => "Testout.xls",};
$process = {data => []}; # need to init process structure with data key
$expected_datastruct = [{col1 => "val11",col3 => "val31"},{col1 => "val12",col3 => "val32"}];
readExcel($File,$process,["Testout.xls"]);
is_deeply($process->{data},$expected_datastruct,"read in excel xls data is expected content");

# read data from excel file using format_headerColumns
$File =  {format_xlformat => "xls", format_skip => 1, format_worksheetID=>1, format_headerColumns => [1,3], format_header => "col1\tcol2\tcol3",format_targetheader => "col1\tcol2\tcol3",filename => "Testout.xls",};
$process = {data => []}; # need to init process structure with data key
$expected_datastruct = []; # expect empty array returned due to error.
readExcel($File,$process,["Testout.xls"]);
is_deeply($process->{data},$expected_datastruct,"read in excel xls data not available due to error");

# read data from XML file
open (FH, ">Testout.xml");
# write test xml
print FH "<topleveldata><coldata>topleveldataVal</coldata><sublevel><datalevel><record><col2>val21</col2><sub><col3>val31</col3></sub></record><record><col2>val22</col2><sub><col3>val32</col3></sub></record></datalevel></sublevel></topleveldata>";
close FH;
$File =  {format_XML => 1, format_sep => ',', format_xpathRecordLevel => '//sublevel/datalevel/*', format_fieldXpath => {col1 => '//topleveldata/coldata', col2 => 'col2', col3 => 'sub/col3'}, format_header => "col1,col2,col3", filename => "Testout.xml",};
$process = {data => []}; # need to init process structure with data key
$expected_datastruct = [{col1 => "topleveldataVal",col2 => "val21",col3 => "val31"},{col1 => "topleveldataVal",col2 => "val22",col3 => "val32"}];
readXML($File,$process,["Testout.xml"]);
is_deeply($process->{data},$expected_datastruct,"read in XML data is expected content");
unlink "Testout.xml";

# read data from XML file with namespace
open (FH, ">Testout.xml");
# write test xml
print FH '<topleveldata xmlns="https://some.funny.namespace"><coldata>topleveldataVal</coldata><sublevel><datalevel><record><col2>val21</col2><sub><col3>val31</col3></sub></record><record><col2>val22</col2><sub><col3>val32</col3></sub></record></datalevel></sublevel></topleveldata>';
close FH;
$File =  {format_XML => 1, format_sep => ',', format_namespaces => {e => 'https://some.funny.namespace'}, format_xpathRecordLevel => '//e:sublevel/e:datalevel/*', format_fieldXpath => {col1 => '//e:topleveldata/e:coldata', col2 => 'e:col2', col3 => 'e:sub/e:col3'}, format_header => "col1,col2,col3", filename => "Testout.xml",};
$process = {data => []}; # need to init process structure with data key
$expected_datastruct = [{col1 => "topleveldataVal",col2 => "val21",col3 => "val31"},{col1 => "topleveldataVal",col2 => "val22",col3 => "val32"}];
readXML($File,$process,["Testout.xml"]);
is_deeply($process->{data},$expected_datastruct,"read in XML data is expected content");

unlink "Testout.txt";
unlink "Testout.csv";
unlink "Testout.xlsx";
unlink "Testout.xls";
unlink "Testout.xml";

# TODO:
# readFile with $redoSubDir

done_testing();