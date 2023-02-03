use strict; use warnings; use Log::Log4perl qw(get_logger); use Log::Log4perl::Level; use Test::More; use Data::Dumper;
use Test::More tests => 2;
use Test::Files;
use File::Spec;
use ETL::Wrap::File; use ETL::Wrap::Common;
Log::Log4perl::init("testlog.config");


# write data to file
my $expected_filecontent = "col1\tcol2\tcol3\nval11\tval21\tval31\nval12\tval22\tval32\n";
my $FileOut = {format => {sep => "\t"},filename => "Testout.txt",columns => {1=>"col1",2=>"col2",3=>"col3"},};
my $processOut = {data => [{col1 => "val11",col2 => "val21",col3 => "val31"},{col1 => "val12",col2 => "val22",col3 => "val32"}]};
writeText($FileOut,$processOut);
file_ok("Testout.txt",$expected_filecontent,"Testout.txt should be written content");

# read data from file
my $FileIn =  {format => {skip => 1, sep => "\t",header => "col1\tcol2\tcol3",targetheader => "col1\tcol2\tcol3",},filename => "Testout.txt",};
my $processIn = {data => []}; # need to init process structure with data key
my $expected_datastruct = [{col1 => "val11",col2 => "val21",col3 => "val31"},{col1 => "val12",col2 => "val22",col3 => "val32"}];
readText($FileIn,$processIn,["Testout.txt"]);
is_deeply($processIn->{data},$expected_datastruct,"read in data should be expected content");

# TODO:
# readFile with $redoSubDir

done_testing();