# WICHTIG: Der aufrufende User (wegen TrustedConnection beim newDBH) sollte dbo in der pubs Datenbank sein, da Tabellen erzeugt/gelöscht werden!

use strict; use warnings; use Log::Log4perl qw(get_logger); use Log::Log4perl::Level; use Test::More; use Data::Dumper;

use ETL::Wrap::DB; use ETL::Wrap::Common;
LogCfgUtil::setupLogging("UnitTestDBUtil");
my $logger = get_logger();

my $dbh = DButil::newDBH("OEBFADBTVI00","pubs",1) or $logger->logexit ("couldn't open DB connection");
my $createStmt = "CREATE TABLE [dbo].[zsysTestTabelle]([Stichtag] [datetime] NOT NULL,[Buchungskreis] [varchar](4) NOT NULL,[Finanzgeschäft] [bigint] NOT NULL,[Produktart] [char](3) NOT NULL,[TeiltilgungLfdNr] [int] NOT NULL,	[Tilgungsbetrag] [decimal](28, 2) NOT NULL, CONSTRAINT [PK_zsysTestTabelle] PRIMARY KEY CLUSTERED (Stichtag ASC)) ON [PRIMARY]";
is(DButil::doInDB($dbh,$createStmt),1,'DButil::doInDB');
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
is(DButil::storeInDB($data,$dbh,"zsysTestTabelle","",1,"Stichtag = ?"),1,'DButil::storeInDB insert');
# upsert
is(DButil::storeInDB($data,$dbh,"zsysTestTabelle","",1,"Stichtag = ?"),1,'DButil::storeInDB upsert');
# Syntaxfehler
is(DButil::storeInDB($data,$dbh,"zsysTestTabelle","",1,"Stich = ?"),0,'DButil::storeInDB Fehler');
# duplikatsfehler
is(DButil::storeInDB($data,$dbh,"zsysTestTabelle","",0,"Stichtag = ?"),0,'DButil::storeInDB duplikatsfehler');
#($data,$dbh,$tableName,$addID,$upsert,$primkey,$ignoreDuplicateErrs,$deleteBeforeInsertSelector,$incrementalStore,$doUpdateBeforeInsert,$debugKeyIndicator) = @_;
# Datenfehler
$data = [{
             'Stichtag' => '20190620',
             'Buchungskreis' => '58ZLZ_VielZuLange',
             'Finanzgeschäft' => 58000456,
			 'Produktart' => 'ZSO',
			 'TeiltilgungLfdNr' => 1,
			 'Tilgungsbetrag' => 123456.12
            }
           ];
is(DButil::storeInDB($data,$dbh,"zsysTestTabelle","",0,"Stichtag = ?",0,"",0,0,"Stichtag=? Finanzgeschäft=?"),0,'DButil::storeInDB Datenfehler');
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
is(DButil::updateInDB($upddata,$dbh,"zsysTestTabelle","Stichtag = ?"),1,'DButil::updateInDB');
my @columnnames;
my $query = "SELECT Stichtag,Buchungskreis,Finanzgeschäft,Produktart,TeiltilgungLfdNr,Tilgungsbetrag from dbo.zsysTestTabelle WHERE Stichtag = '20190619'";
my $result = DButil::readFromDB($dbh,$query,\@columnnames) or $logger->logexit ("konnte nicht aus Datenbank lesen ...");
is($result->[0]{"TeiltilgungLfdNr"},1,'DButil::readFromDB');
is("@columnnames","Stichtag Buchungskreis Finanzgeschäft Produktart TeiltilgungLfdNr Tilgungsbetrag","columnnames from readFromDB");
DButil::doInDB($dbh,"DROP TABLE [dbo].[zsysTestTabelle]");

my @retvals;
DButil::doInDB($dbh,"sp_helpdb ?",\@retvals,"InfoDB");
# Rückgabe: array von array referenzen, die wiederum hash refs enthalten: Rückgabe mehrfacher Datensets der stored procedure
is($retvals[0]->[0]->{"name"},"InfoDB","Rückgabe mehrfacher Datensets aus stored proc mit parametern");
# ref auf array in zweiter Rückgabe auf array dereferenzieren um auf die Anzahl der records zu kommen.
is(scalar(@{$retvals[1]}),2,"Rückgabe mehrfacher Datensets aus stored proc mit parametern");
DButil::doInDB($dbh,"sp_helpdb ?","","InfoDB");

# TODO:
# beginWork
# commit
# rollback
# readFromDBHash
# deleteFromDB
# 
# Roundtrip test: Daten schreiben und wieder auslesen, Vergleich ob gleich
$dbh->disconnect;
done_testing();