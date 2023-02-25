use strict; use warnings; use Log::Log4perl qw(get_logger); use Log::Log4perl::Level; use Test::More; use Data::Dumper;
use ETL::Wrap; use Test::Files; use File::Spec;
use Test::More tests => 1;

Log::Log4perl::init("testlog.config");
my $logger = get_logger();

my ($expected_filecontent,$expected_datastruct,$File,$process);

done_testing();