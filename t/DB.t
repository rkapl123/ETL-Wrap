# calling user (because of TrustedConnection with newDBH) should be dbo in the pubs database, as tables are created/dropped
use strict; use warnings; use Log::Log4perl qw(get_logger); use Log::Log4perl::Level; use Test::More; use Data::Dumper;
use ETL::Wrap::DB;
use Test::More tests => 12;

Log::Log4perl::init("testlog.config");
my $logger = get_logger();

ETL::Wrap::DB::newDBH("lenovo-pc","pubs",1) or $logger->logexit ("couldn't open DB connection");
my $createStmt = "CREATE TABLE [dbo].[TestTabelle]([selDate] [datetime] NOT NULL,[ID0] [varchar](4) NOT NULL,[ID1] [bigint] NOT NULL,[ID2] [char](3) NOT NULL,[Number] [int] NOT NULL,	[Amount] [decimal](28, 2) NOT NULL, CONSTRAINT [PK_TestTabelle] PRIMARY KEY CLUSTERED (selDate ASC)) ON [PRIMARY]";
is(ETL::Wrap::DB::doInDB($createStmt),1,'doInDB');
my $data = [{
             'selDate' => '20190619',
             'ID0' => 'ABCD',
             'ID1' => 5456,
			 'ID2' => 'ZYX',
			 'Number' => 1,
			 'Amount' => 123456.12
            },
            {
             'selDate' => '20190619',
             'ID0' => 'ABCD',
             'ID1' => 5856,
			 'ID2' => 'XYY',
			 'Number' => 1,
			 'Amount' => 123456.12
            },
           ];
# insert
is(ETL::Wrap::DB::storeInDB($data,"TestTabelle","",1,"selDate = ?"),1,'storeInDB insert');
# upsert                          
is(ETL::Wrap::DB::storeInDB($data,"TestTabelle","",1,"selDate = ?"),1,'storeInDB upsert');
# Syntax error                    
is(ETL::Wrap::DB::storeInDB($data,"TestTabelle","",1,"selDt = ?"),0,'storeInDB error');
# duplicate error                 
is(ETL::Wrap::DB::storeInDB($data,"TestTabelle","",0,"selDate = ?"),0,'storeInDB duplicate error');

# Data error
$data = [{
             'selDate' => '20190620',
             'ID0' => 'ABCD_WayTooLongField',
             'ID1' => 5456,
			 'ID2' => 'XZY',
			 'Number' => 1,
			 'Amount' => 123456.12
            }
           ];
is(ETL::Wrap::DB::storeInDB($data,"TestTabelle","",0,"selDate = ?",0,"",0,0,"selDate=? ID1=?"),0,'storeInDB Datenfehler');
# update in Database
my $upddata = {'20190619' => {
             'selDate' => '20190619',
             'ID0' => 'ABCD',
             'ID1' => 5856,
			 'ID2' => 'XYZ',
			 'Number' => 1,
			 'Amount' => 123456.12
           }
         };
is(ETL::Wrap::DB::updateInDB($upddata,"TestTabelle","selDate = ?"),1,'updateInDB');
my @columnnames;
my $query = "SELECT selDate,ID0,ID1,ID2,Number,Amount from dbo.TestTabelle WHERE selDate = '20190619'";
my $result = DButil::readFromDB($query,\@columnnames) or $logger->logexit ("couldn't read from Database");
is($result->[0]{"Number"},1,'DButil::readFromDB');
is("@columnnames","selDate ID0 ID1 ID2 Number Amount","columnnames from readFromDB");
DButil::doInDB("DROP TABLE [dbo].[TestTabelle]");

my @retvals;
ETL::Wrap::DB::doInDB("sp_helpdb ?",\@retvals,"InfoDB");
# Rückgabe: array von array referenzen, die wiederum hash refs enthalten: Rückgabe mehrfacher Datensets der stored procedure
is($retvals[0]->[0]->{"name"},"InfoDB","Rückgabe mehrfacher Datensets aus stored proc mit parametern");
# ref auf array in zweiter Rückgabe auf array dereferenzieren um auf die Anzahl der records zu kommen.
is(scalar(@{$retvals[1]}),2,"Rückgabe mehrfacher Datensets aus stored proc mit parametern");
ETL::Wrap::DB::doInDB("sp_helpdb ?","","InfoDB");

# TODO:
# beginWork
# commit
# rollback
# readFromDBHash
# deleteFromDB
# 
# Roundtrip test: write data adn read back again, compare
done_testing();