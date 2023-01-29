use strict; use warnings; use Test::More;
use ETL::Wrap::DateUtil; use Time::Piece;

my @res = ("20150102","20150105","20150107","20150108","20150109","20150112","20150113","20150114","20150115","20150116","20150119","20150120","20150121","20150122","20150123","20150126","20150127","20150128","20150129","20150130","20150202","20150203","20150204","20150205","20150206","20150209","20150210","20150211","20150212","20150213","20150216","20150217","20150218","20150219","20150220","20150223","20150224","20150225","20150226","20150227","20150302");
is(get_dateseries("20150102","20150302","AT"),@res,'get_dateseries');
is(is_weekend("20150102"),"",'is_weekend');
is(is_weekend("20150103"),1,'is_weekend');
is(is_weekend("20150104"),1,'is_weekend');
is(is_weekend("20150105"),"",'is_weekend');
is(weekday("20180801"),4,'weekday');
is(weekday("20180802"),5,'weekday');
is(weekday("20180803"),6,'weekday');
is(weekday("20180804"),7,'weekday');
is(weekday("20180805"),1,'weekday');
is(weekday("20180806"),2,'weekday');
is(weekday("20180807"),3,'weekday');
is(addMonths("20121030",1),"20121130",'addMonths 20121030 + 1 = 20121130');
is(addMonths("20121030",3),"20130130",'addMonths 20121030 + 3 = 20130130');
is(addMonths("20121030",13),"20131130",'addMonths 20121030 + 13 = 20131130');
is(addMonths("20121030",14),"20131230",'addMonths 20121030 + 14 = 20131230');
is(addMonths("20121030",16),"20140302",'addMonths 20121030 + 16 = 20140302 nicht 20140230 !!');
is(addDaysHol("20111231",1),"20120102",'addDaysHol (default AT)');
is(addDaysHol("20120105",1),"20120109",'addDaysHol (default AT)');
is(addDaysHol("20120430",1),"20120502",'addDaysHol (default AT)');
is(addDaysHol("20120814",1),"20120816",'addDaysHol (default AT)');
is(addDaysHol("20121025",1),"20121029",'addDaysHol (default AT)');
is(addDaysHol("20121031",1),"20121102",'addDaysHol (default AT)');
is(addDaysHol("20121207",1),"20121210",'addDaysHol (default AT)');
is(addDaysHol("20121224",1),"20121227",'addDaysHol (default AT)');
is(addDaysHol("20121224",1,"YMD","WE"),"20121225",'addDaysHol nur Wochenende');
is(addDaysHol("20220429",1,"YMD","NO"),"20220430",'addDaysHol kein Wochenende, kein Feiertag');
is(addDaysHol("20120405",1,"D.M.Y"),  "06.04.2012",'addDaysHol (default AT) mit Format');
is(addDaysHol("20120408",1,"D-M-Y"),  "10-04-2012",'addDaysHol (default AT) mit Format');
is(addDaysHol("20120516",1,"Y-M-D"),  "2012-05-18",'addDaysHol (default AT) mit Format');
is(addDaysHol("20120527",1,"D-MMM-Y"), "29-May-2012",'addDaysHol (default AT) mit Format');
is(addDaysHol("20120606",1,"D/M/Y"),  "08/06/2012",'addDaysHol (default AT) mit Format');
is(addDaysHol("20121224",1,"D.mmm.Y"),"27.Dez.2012",'addDaysHol (default AT) mit Format');
is(subtractDaysHol("20120102",1),"20111230",'subtractDaysHol (default AT)');
is(subtractDaysHol("20120502",1),"20120430",'subtractDaysHol (default AT)');
is(subtractDaysHol("20121224",1,"YMD","WE"),"20121221",'subtractDaysHol Kalender "nur Wochenende"');
is(subtractDaysHol("20220502",1,"YMD","NO"),"20220501",'subtractDaysHol NO (kein Kalender)');
is(subtractDaysHol("20191227",1,"YMD","BF"),"20191223",'subtractDaysHol Kalender BF Weihnachten');
is(is_holiday("BF", subtractDaysHol("20191227",1,"YMD","WE")),1,'gestern Feiertag');
is(is_holiday("BF", subtractDaysHol("20191226",1,"YMD","WE")),1,'gestern Feiertag');
is(is_holiday("BF", subtractDaysHol("20191225",1,"YMD","WE")),1,'gestern Feiertag');
is(is_holiday("BF", subtractDaysHol("20191224",1,"YMD","WE")),0,'gestern kein Feiertag');
is(is_holiday("BF", subtractDaysHol("20191223",1,"YMD","WE")),0,'gestern kein Feiertag');
is(is_holiday("BF", subtractDaysHol("20191209",1,"YMD","WE")),0,'gestern kein Feiertag');
my ($day,$month,$year) = (1,1,2007);
is(addDays(\$day, \$month, \$year, 1),"02-Jan-2007",'addDays');
is($day, 2,'addDays day');
is($month, 1,'addDays month');
is($year, 2007,'addDays year');
# 7. Mai 2018 -> erster Montag?
is(first_week(7,5,2018,1,5), 1,'first_week May');
is(first_weekYYYYMMDD("20180507",1,5), 1,'Mon first_week May YYYYMMDD monday');
is(first_weekYYYYMMDD("20210105",2), 1,'Tue first_week YYYYMMDD wednesday without month given');
is(first_weekYYYYMMDD("20210106",3), 1,'Wen first_week YYYYMMDD wednesday without month given');
is(first_weekYYYYMMDD("20210107",4), 1,'Thu first_week YYYYMMDD wednesday without month given');
is(first_weekYYYYMMDD("20210101",5), 1,'Fri first_week YYYYMMDD wednesday without month given');
is(first_weekYYYYMMDD("20210102",6), 1,'Sat first_week YYYYMMDD wednesday without month given');
is(first_weekYYYYMMDD("20210103",0), 1,'Sun first_week YYYYMMDD wednesday without month given');
is(first_weekYYYYMMDD("20180507",1), 1,'Mon first_week YYYYMMDD without month given');
# 28. Mai 2018 -> letzter Montag?
is(last_week(28,5,2018,1,5), 1,'Mon last_week May');
# 27. Aug 2018 -> letzter Montag?
is(last_week(27,8,2018,1,8), 1,'Mon last_week Aug');
# 28. Dez 2018 -> letzter Freitag?
is(last_week(28,12,2018,5,12), 1,'last_week Dec');
is(last_weekYYYYMMDD("20181228",5,12), 1,'Fri last_week Dec YYYYMMDD');
is(last_weekYYYYMMDD("20181228",5), 1,'Fri last_week Dec YYYYMMDD without month given');
# 7. jan 2018 -> erster sonntag?
is(first_week(7,1,2018,0,1), 1,'first_week Jan');
for my $cal ("BS","BF","AT","TG","UK") {
	is(is_holiday($cal,"20180101"),1,'is_holiday '.$cal);
	is(is_holiday($cal,"20181225"),1,'is_holiday '.$cal);
	is(is_holiday($cal,"20181226"),1,'is_holiday '.$cal);
	is(is_holiday($cal,"20180502"),0,'is_holiday '.$cal);
}
is(is_holiday("AT","20120101"),1,'Neujahr AT');
is(is_holiday("AT","20120106"),1,'hl 3 k�nige AT');
is(is_holiday("AT","20120409"),1,'is_holiday AT');
is(is_holiday("AT","20120501"),1,'tag der arbeit AT');
is(is_holiday("AT","20120517"),1,'is_holiday AT');
is(is_holiday("AT","20120528"),1,'is_holiday AT');
is(is_holiday("AT","20120607"),1,'is_holiday AT');
is(is_holiday("AT","20120815"),1,'Maria Himmelf. AT');
is(is_holiday("AT","20121026"),1,'Nationalfeiertag AT');
is(is_holiday("AT","20121101"),1,'Allerheiligen AT');
is(is_holiday("AT","20121208"),1,'Maria Empf. AT');
is(is_holiday("AT","20121224"),0,'heiligabend AT');
is(is_holiday("AT","20121224"),0,'heiligabend BS');
is(is_holiday("AT","20121224"),0,'heiligabend BF');
is(is_holiday("AT","20121225"),1,'christtag AT');
is(is_holiday("AT","20121226"),1,'stefani AT');
is(is_holiday("AT","20120406"),0,'karfreitag AT');
is(is_holiday("BS","20120406"),1,'karfreitag BS');
is(is_holiday("BF","20120406"),0,'karfreitag BF');
is(is_holiday("UK","20180507"),1,'may day UK');
is(is_holiday("UK","20180528"),1,'spring bank holiday UK');
is(is_holiday("UK","20180827"),1,'summer bank holiday UK');
is(is_holiday("UK","20180501"),0,'no labour day in uk');
is(is_holiday("TG","20180507"),0,'no may day in Target');
is(is_holiday("TG","20180528"),0,'no spring bank holiday day in Target');
is(is_holiday("TG","20180827"),0,'no summer bank holiday day in Target');
is(is_holiday("TG","20180101"),1,'Neujahr Target');
is(is_holiday("TG","20180501"),1,'tag der arbeit Target');
is(is_holiday("TG","20181225"),1,'christtag Target');
is(is_holiday("TG","20181226"),1,'stefani Target');
is(is_holiday("AT","20210405"),1,'Ostermontag');
is(is_first_day_of_month("20190101"),1,'is_first_day_of_month true');
is(is_first_day_of_month("20190102"),0,'is_first_day_of_month false');
is(is_last_day_of_month("20190131"),1,'is_last_day_of_month true');
is(is_last_day_of_month("20190130"),0,'is_last_day_of_month false');
is(is_last_day_of_month("20190228"),1,'is_last_day_of_month feb normal');
# is(is_last_day_of_month("20190229"),0,'is_last_day_of_month invalid date'); # ung�ltige Datumswerte k�nnen nicht abgefangen werden !!
is(is_last_day_of_month("20200229"),1,'is_last_day_of_month feb schaltjahr');
is(is_last_day_of_month("20200228"),0,'is_last_day_of_month 28 feb schaltjahr');
is(is_last_day_of_month("20220429","WE"),1,'is_last_day_of_month 29 April 22 = Freitag');
is(is_last_day_of_month("20220428","WE"),0,'is_last_day_of_month 28 April 22 = Donnerstag');
is(first_week(6,5,2019,1,5),1,'first_week 1 = erster Montag im Mai');
is(first_week(7,5,2019,1,5),0,'first_week false: nicht montag');
is(first_week(13,5,2019,1,5),0,'first_week false: nicht erster montag');
is(last_week(27,5,2019,1,5),1,'last_week 1=letzter Montag im Mai');
is(last_week(10,5,2019,1,5),0,'last_week false');
like(get_curdate,qr/\d{8}/,'get_curdate');
like(get_curdatetime,qr/\d{8}_\d{6}/,'get_curdatetime');
like(get_curdate_dot,qr/\d{2}\.\d{2}\.20\d{2}/,'get_curdate_dot');
is(formatDate(2019,1,1,"D.M.Y"),"01.01.2019",'formatDate D.M.Y');
is(formatDate(2019,3,1,"D.MMM.Y"),"01.Mar.2019",'formatDate D.MMM.Y');
is(formatDate(2019,3,1,"D.mmm.Y"),"01.M�r.2019",'formatDate D.mmm.Y');
is(formatDateFromYYYYMMDD("20190101","D.M.Y"),"01.01.2019",'formatDateFromYYYYMMDD D.M.Y');
is(get_curdate_dash_plus_X_years(100,"20190101"),"01-01-2119",'get_curdate_dash_plus_X_years mit datum');
is(get_curdate_dash_plus_X_years(100,"20190105",4),"01-01-2119",'get_curdate_dash_plus_X_years mit datum und Tage abziehen');
# nur "like" auf format, da diese funktionen �ndernde werte zur�ckgeben.
like(get_curdate_dash(),qr/\d{2}\-\d{2}\-20\d{2}/,'get_curdate_dash');
like(get_curdate_dash_plus_X_years(100),qr/\d{2}\-\d{2}\-21\d{2}/,'get_curdate_dash_plus_X_years ohne datum');
like(get_curtime(),qr/\d{2}:\d{2}:\d{2}/,'get_curtime');
like(get_curtime("%02d_%02d_%02d"),qr/\d{2}_\d{2}_\d{2}/,'get_curtime mit format %02d_%02d_%02d');
like(get_curtime("%02d%02d%02d"),qr/\d{2}\d{2}\d{2}/,'get_curtime mit format %02d%02d%02d');
print "get_curtime HHMM:".get_curtime("%02d%02d")."\n";
like(get_curtime("%02d%02d"),qr/\d{2}\d{2}/,'get_curtime mit format %02d%02d');
like(get_curtime_HHMM(),qr/\d{4}/,'get_curtime_HHMM');
like(get_curdate_gen("D.M.Y"),qr/\d{2}\.\d{2}\.20\d{2}/,'get_curdate_gen D.M.Y');
like(get_curdate_gen("D/M/Y"),qr/\d{2}\/\d{2}\/20\d{2}/,'get_curdate_gen D/M/Y');
like(get_curdate_gen("YMD"),qr/20\d{6}/,'get_curdate_gen YMD');
like(get_curdate_gen(),qr/20\d{6}/,'get_curdate_gen default = YMD');
like(get_curdate_gen("D-MMM-Y"),qr/\d{2}-\w{3}-20\d{2}/,'get_curdate_gen D-MMM-Y');
is(convertToThousendDecimal(123456789.12),"123.456.789,12",'convertToThousendDecimal kommazahl');
is(convertToThousendDecimal(123456789),"123.456.789,0",'convertToThousendDecimal ganzzahl');
is(convertToThousendDecimal(0),"0,0",'convertToThousendDecimal 0 mit nachkomma');
is(convertToThousendDecimal(0,1),"0",'convertToThousendDecimal 0 ohne nachkomma');
is(convertToThousendDecimal(12345.20,1),"12.345",'convertToThousendDecimal kommazahl ohne nachkomma');
is(parseFromDDMMYYYY("01.01.1970"),-3600,'parseFromDDMMYYYY 01.01.1970');
is(parseFromDDMMYYYY("02.01.1970"),-3600+24*60*60,'parseFromDDMMYYYY 02.01.1970');
is(parseFromYYYYMMDD("19700102"),-3600+24*60*60,'parseFromYYYYMMDD 19700102');
is((parseFromYYYYMMDD("19700103")-parseFromYYYYMMDD("19700101"))/(24*60*60),2,'differenz zwischen 19700103 - 19700101 in Tagen');
is((parseFromYYYYMMDD("20191104")-parseFromDDMMYYYY("01.11.2019"))/(24*60*60),3,'differenz zwischen 20191104 - 01.11.2019 in Tagen');
is(parseFromYYYYMMDD("19000100"),"invalid date",'Erwarteter Fehler bei ungueltigem Argument (Jahr >= 1900, 1<=Monat<=12, 1<=Tag<=31): Rueckgabe 0');
is(parseFromDDMMYYYY("01.13.2001"),"invalid date",'Erwarteter Fehler bei ungueltigem Argument (Jahr >= 1900, 1<=Monat<=12, 1<=Tag<=31): Rueckgabe 0');
is(parseFromYYYYMMDD(""),"invalid date",'Erwarteter Fehler bei ungueltigem Argument (Jahr >= 1900, 1<=Monat<=12, 1<=Tag<=31): Rueckgabe 0');
is(parseFromDDMMYYYY("01.01.1801"),"invalid date",'Erwarteter Fehler bei ungueltigem Argument (Jahr >= 1900, 1<=Monat<=12, 1<=Tag<=31): Rueckgabe 0');
is(parseFromYYYYMMDD("20010132"),"invalid date",'Erwarteter Fehler bei ungueltigem Argument (Jahr >= 1900, 1<=Monat<=12, 1<=Tag<=31): Rueckgabe 0');
is(parseFromDDMMYYYY("00.01.1901"),"invalid date",'Erwarteter Fehler bei ungueltigem Argument (Jahr >= 1900, 1<=Monat<=12, 1<=Tag<=31): Rueckgabe 0');
is(convertEpochToYYYYMMDD(parseFromYYYYMMDD("20010131")),"20010131",'convertEpochToYYYYMMDD 20010131');
is(convertEpochToYYYYMMDD(Time::Piece->strptime("20010131","%Y%m%d")),"20010131",'convertEpochToYYYYMMDD Time::Piece 20010131');
is(get_last_day_of_month("20011215"),"20011231",'get_last_day_of_month 20011231');
is(get_last_day_of_month("20010115"),"20010131",'get_last_day_of_month 20010131');
is(get_last_day_of_month("20010215"),"20010228",'get_last_day_of_month 20010228');
is(get_last_day_of_month("20040215"),"20040229",'get_last_day_of_month 20040229');
done_testing();