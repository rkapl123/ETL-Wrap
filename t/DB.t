# calling user (because of TrustedConnection with newDBH) should be dbo in the pubs database, as tables are created/dropped
use strict; use warnings; use Log::Log4perl qw(get_logger); use Log::Log4perl::Level; use Test::More; use Data::Dumper;

use ETL::Wrap::DB; use ETL::Wrap::Common;
Log::Log4perl::init("testlog.config");
my $logger = get_logger();

ETL::Wrap::DB::newDBH("lenovo-pc","pubs",1) or $logger->logexit ("couldn't open DB connection");
my $createStmt = "CREATE TABLE [dbo].[zsysTestTabelle]([Stichtag] [datetime] NOT NULL,[Buchungskreis] [varchar](4) NOT NULL,[Finanzgeschäft] [bigint] NOT NULL,[Produktart] [char](3) NOT NULL,[TeiltilgungLfdNr] [int] NOT NULL,	[Tilgungsbetrag] [decimal](28, 2) NOT NULL, CONSTRAINT [PK_zsysTestTabelle] PRIMARY KEY CLUSTERED (Stichtag ASC)) ON [PRIMARY]";
is(ETL::Wrap::DB::doInDB($dbh,$createStmt),1,'doInDB');
my $data = [{
             'Stichtag' => '20190619',
             'Buchungskreis' => '58ZL',
             'Finanzgeschäft' => 58000456,
			 'Produktart' => 'ZSO',
			 'TeiltilgungLfdNr' => 1,
			 'Tilgungsbetrag' => 123456.12
            },
            {
             'Stichtag' => '20190619',
             'Buchungskreis' => '58ZL',
             'Finanzgeschäft' => 58000456,
			 'Produktart' => 'ZSO',
			 'TeiltilgungLfdNr' => 1,
			 'Tilgungsbetrag' => 123456.12
            },
           ];
# insert
is(ETL::Wrap::DB::storeInDB($data,$dbh,"zsysTestTabelle","",1,"Stichtag = ?"),1,'storeInDB insert');
# upsert
is(ETL::Wrap::DB::storeInDB($data,$dbh,"zsysTestTabelle","",1,"Stichtag = ?"),1,'storeInDB upsert');
# Syntax error
is(ETL::Wrap::DB::storeInDB($data,$dbh,"zsysTestTabelle","",1,"Stich = ?"),0,'storeInDB error');
# duplicate error
is(ETL::Wrap::DB::storeInDB($data,$dbh,"zsysTestTabelle","",0,"Stichtag = ?"),0,'storeInDB duplicate error');
#($data,$dbh,$tableName,$addID,$upsert,$primkey,$ignoreDuplicateErrs,$deleteBeforeInsertSelector,$incrementalStore,$doUpdateBeforeInsert,$debugKeyIndicator) = @_;
# Data error
$data = [{
             'Stichtag' => '20190620',
             'Buchungskreis' => '58ZLZ_VielZuLange',
             'Finanzgeschäft' => 58000456,
			 'Produktart' => 'ZSO',
			 'TeiltilgungLfdNr' => 1,
			 'Tilgungsbetrag' => 123456.12
            }
           ];
is(ETL::Wrap::DB::storeInDB($data,"zsysTestTabelle","",0,"Stichtag = ?",0,"",0,0,"Stichtag=? Finanzgeschäft=?"),0,'storeInDB Datenfehler');
# update in Database
my $upddata = {'20190619' => {
             'Stichtag' => '20190619',
             'Buchungskreis' => '58ZL',
             'Finanzgeschäft' => 58000456,
			 'Produktart' => 'KSE',
			 'TeiltilgungLfdNr' => 1,
			 'Tilgungsbetrag' => 123456.12
           }
         };
is(ETL::Wrap::DB::updateInDB($upddata,"zsysTestTabelle","Stichtag = ?"),1,'updateInDB');
my @columnnames;
my $query = "SELECT Stichtag,Buchungskreis,Finanzgeschäft,Produktart,TeiltilgungLfdNr,Tilgungsbetrag from dbo.zsysTestTabelle WHERE Stichtag = '20190619'";
my $result = DButil::readFromDB($query,\@columnnames) or $logger->logexit ("konnte nicht aus Datenbank lesen ...");
is($result->[0]{"TeiltilgungLfdNr"},1,'DButil::readFromDB');
is("@columnnames","Stichtag Buchungskreis Finanzgeschäft Produktart TeiltilgungLfdNr Tilgungsbetrag","columnnames from readFromDB");
DButil::doInDB("DROP TABLE [dbo].[zsysTestTabelle]");

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
# Roundtrip test: Daten schreiben und wieder auslesen, Vergleich ob gleich
done_testing();