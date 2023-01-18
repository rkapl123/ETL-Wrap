package ETL::Wrap::FTP;

use strict; 
use Log::Log4perl qw(get_logger); use File::Temp;
use Net::SFTP::Foreign; use Data::Dumper; use Net::SFTP::Foreign::Constants qw( SFTP_ERR_LOCAL_UTIME_FAILED );use Fcntl ':mode';use Time::Local; use Time::localtime; use Exporter;
use ETL::Wrap::DateUtil;# use ETL::Wrap::Common;
use Win32::ShellQuote qw(:all); # for ugly passwords that contain <#|>% .

our @ISA = qw(Exporter);
our @EXPORT = qw(removeFilesOlderX fetchFiles writeFiles moveTempFiles archiveFiles login);

my $ftp;
my $RemoteHost;

# remove all files in FTP server folders that are older than a given day/month/year
sub removeFilesOlderX {
	my ($FTP) = @_;
	my $logger = get_logger();
	$logger->info("remove files in FTP (archive)folder older than ".$FTP->{remove}{day}." days, ".$FTP->{remove}{mon}." months and ".$FTP->{remove}{year})." years";
	if (defined $ftp) {
		for my $folder ($FTP->{remove}{removeFolders}) {
			my $mtimeToKeep = timelocal(0,0,0,localtime->mday()-$FTP->{remove}{day},localtime->mon()-$FTP->{remove}{mon},localtime->year()+1900-$FTP->{remove}{year});
			$logger->debug("changing into FTP RemoteDir [".$FTP->{RemoteDir}."]");
			if ($ftp->setcwd($FTP->{RemoteDir}.$folder)) {
				my $files = $ftp->ls('.',
									wanted => sub {
										# callback function is being passed the read files in a reference to hash with 3 keys:
										# filename, longname (as from ls -l) and "a", which is a Net::SFTP::Foreign::Attributes object that contains atime, mtime, permissions and size of the file.
										my $attr = $_[1]->{a}; 
										# if "wanted" returns true (mod time is older than mtimeToKeep), then the file is being added to the returned array
										$logger->trace("file: ".$_[1]->{filename}.",mtime: ".$attr->mtime.",mtimeToKeep: ".$mtimeToKeep);
										return $attr->mtime < $mtimeToKeep && S_ISREG($attr->perm); # add file if mod time < required mod time AND it is a regular file...
									} ) or $logger->error("Kann file liste nicht holen, Ursache: ".$ftp->error.", status: ".$ftp->status);
				for my $file (@$files) {
					$logger->info(($FTP->{simulate} ? "simulate removal of: " : "removing: ").$file->{filename});
					unless ($FTP->{simulate}) {
						$ftp->remove($file->{filename}) or $logger->error("can't remove '".$file->{filename}."': ".$ftp->error.", status: ".$ftp->status);
					}
				}
				$logger->info("no Files to remove !") if !@$files;
				$logger->info("remove files finished ...");
			} else {
				$logger->error("can't cwd to remove folder ".$FTP->{RemoteDir}.$folder.": ".$ftp->error.", status: ".$ftp->status);
				return 0;
			}
		}
	} else {
		$logger->error("no ftp connection opened!");
		return 0;
	}
	return 1;
}

# get files from FTP server
sub getFiles {
	my ($FTP,$execute,$param) = @_;
	my $logger = get_logger();
	my $suppressGetError = $execute->{firstRunSuccess};
	my $queue_size = $FTP->{queue_size};
	$queue_size = 1 if !$queue_size; # queue_size bigger 1 causes oft connection issues
	if (defined $ftp) {
		$logger->debug("changing into folder [".$FTP->{remoteDir}."]");
		if ($ftp->setcwd($FTP->{remoteDir})) {
			my $remoteFile = ($FTP->{path} ? $FTP->{path}."/" : "").$param->{fileToRetrieve};
			my $localPath = ($FTP->{localDir} ? $FTP->{localDir} : $execute->{homedir});
			$localPath .= "/" if $localPath !~ /.*[\/\\]$/;
			my $localFile = $localPath.$param->{fileToRetrieve};
			$logger->info("fetching file/fileglob $remoteFile");
			if ($remoteFile =~ /\*/) { # if there is a glob character then do mget !
				my @multipleFiles = $ftp->glob($remoteFile, names_only => 1); # list retrieved files for later processing, names_only => 1 for getting only the filename
				@{$execute->{retrievedFiles}} = @multipleFiles;
				$ftp->mget($remoteFile, $localPath, queue_size => $queue_size);
				if ($ftp->error != 0) {
					unless ($ftp->error == SFTP_ERR_LOCAL_UTIME_FAILED || $suppressGetError) {
						$logger->error("error: can't get remote-file ".$remoteFile." (in ".$ftp->cwd()."), reason: ".$ftp->error.", status: ".$ftp->status);
						return 0;
					}
				} else {
					$logger->info("mget files $remoteFile into $FTP->{localDir}: ".@multipleFiles);
				}
			} else {
				my $attr = $ftp->stat($remoteFile);
				my $mod_time = $attr->mtime if $attr;
				$logger->trace(Dumper($attr));
				$logger->debug("get file $remoteFile ...");
				if (!$ftp->get($remoteFile, $localFile, queue_size => $queue_size)) { # copy_time => 0
					if (!$param->{fileToRetrieveOptional} and !$FTP->{filenameToRemove}) { # ignore errors for a file that was either removed or is optional 
						unless ($ftp->error == SFTP_ERR_LOCAL_UTIME_FAILED || $suppressGetError) {
							$logger->warn("error: SFTP_ERR_LOCAL_UTIME_FAILED, sftp->status:".$ftp->status);
							$logger->error("error: can't get remote-file ".$remoteFile." (in ".$ftp->cwd()."), reason: ".$ftp->error);
							return 0;
						}
					}
				} else {
					@{$execute->{retrievedFiles}} = ($param->{fileToRetrieve});
					$logger->info("fetched file $remoteFile to $localFile");
				}
				if ($mod_time && !$FTP->{dontDoUtime}) {
					 utime($mod_time,$mod_time,$localFile) or $logger->warn("couldn't set time for $localFile: $!");
				}
			}
		} else {
			$logger->error("can't change into remote-directory ".$FTP->{remoteDir}.", reason: ".$ftp->error);
			return 0;
		}
	} else {
		$logger->error("no ftp connection opened!");
		return 0;
	}
	return 1;
}

# upload files to FTP server
sub uploadFile {
	my ($FTP,$param) = @_;
	my $logger = get_logger();
	my $localFile = $param->{fileToWrite} or do {
		$logger->error("no file to upload (fileToWrite parameter) !");
		return 0;
	};
	$logger->info("uploading file $localFile");
	if (defined $ftp) {
		my $doSetStat = ($FTP->{dontDoSetStat} ? 0 : 1);
		$logger->debug("changing into folder [".$FTP->{remoteDir}."]");
		if ($ftp->setcwd($FTP->{remoteDir})) {
			if ($FTP->{dontUseTempFile}) {
				$logger->info("uploading file $localFile , doSetStat: $doSetStat ...");
				if (!$ftp->put($localFile, $localFile, late_set_perm => 1, copy_perm => $doSetStat, copy_time => $doSetStat)) {
					$logger->error("error: can't upload local file ".$FTP->{remoteDir}.$localFile.", reason: ".$ftp->error);
					return 0;
				}
			} else {
				# sichere methode für den upload falls ein monitor "zuhört": upload eines temp file, dann remote auf finalen namen umbenennen.
				# zuerst lokal auf temp... umbenennen
				rename $localFile, "temp.".$localFile or $logger->error("error: can't rename local file ".$localFile." to temp.".$localFile.", reason: ".$!) ;
				$logger->info("Sftp: hinaufladen von file temp.$localFile ...");
				if (!$ftp->put("temp.".$localFile, "temp.".$localFile)) {
					$logger->error("error: can't upload local temp file to ".$FTP->{remoteDir}."temp.".$localFile.", reason: ".$ftp->error);
					return 0;
				}
				# dann remote richtigstellen
				if (!$FTP->{dontMoveTempImmediately}) {
					$logger->debug("Sftp: remote umbenennen temp file temp.$localFile auf $localFile ...");
					if ($ftp->rename("temp.".$localFile,$localFile)) {
						$logger->debug("Sftp: temporäres file ".$FTP->{remoteDir}."temp.".$localFile." umbenannt auf ".$localFile);
					} else {
						my $errmsg = $ftp->error;
						$logger->error("error: can't rename remote-file ".$FTP->{remoteDir}."temp.".$localFile." to ".$localFile.", reason: ".$errmsg) ;
					}
				}
				# zuletzt auch lokal zurückbenennen damit normal weiterverarbeitet werden kann
				rename "temp.".$localFile, $localFile;
			}
		} else {
			$logger->error("can't change into remote-directory ".$FTP->{remoteDir}.", reason: ".$ftp->error);
			return 0;
		}
	} else {
		$logger->error("no ftp connection opened!");
		return 0;
	}
	return 1;
}

# move temp files on FTP server
sub moveTempFiles {
	my ($FTP, $param) = @_;
	my $logger = get_logger();
	my $localFile = $param->{fileToWrite} or do {
		$logger->error("no file to upload (fileToWrite parameter) !");
		return 0;
	};
	$logger->info("final rename of temp Files for $localFile");
	if (defined $ftp) {
		$logger->debug("changing into folder [".$FTP->{remoteDir}."]");
		if ($ftp->setcwd($FTP->{remoteDir})) {
			if ($ftp->rename("temp.".$localFile,$localFile)) {
				$logger->debug("temporary file ".$FTP->{remoteDir}."temp.".$localFile." renamed to ".$localFile);
			} else {
				my $errmsg = $ftp->error;
				$logger->error("error: can't rename remote-file ".$FTP->{remoteDir}."temp.".$localFile." to ".$localFile.", reason: ".$errmsg);
				return 0;
			}
		} else {
			$logger->error("can't change into remote-directory ".$FTP->{remoteDir}.", reason: ".$ftp->error);
			return 0;
		}
	} else {
		$logger->error("no ftp connection opened!");
		return 0;
	}
	return 1;
}

# move files into defined archive folder or delete them
sub archiveFiles {
	my ($FTP, $param) = @_;
	my $logger = get_logger();
	my @filesToRemove = @{$param->{filesToRemove}};
	my @filesToArchive = @{$param->{filesToArchive}};
	my $archiveTimestamp = get_curdatetime();;
	
	if (defined $ftp) {
		$logger->debug("changing into folder [".$FTP->{remoteDir}."]");
		if ($ftp->setcwd($FTP->{remoteDir})) {
			$logger->info("removing files @filesToRemove ...") if @filesToRemove;
			for my $remoteFile (@filesToRemove) {
				if ($ftp->remove($remoteFile)) {
					$logger->debug("removed remote-file ".$FTP->{remoteDir}."/".$remoteFile.".");
				} else {
					my $errmsg = $ftp->error;
					$logger->error("error: can't remove remote-file ".$FTP->{remoteDir}."/".$remoteFile.", reason: ".$errmsg) if $errmsg !~ /No such file or directory/;
					$logger->warn("error: ".$errmsg) if $errmsg =~ /No such file or directory/;
				}
			}
			$logger->info("archiviing files @filesToArchive to ".$FTP->{archiveFolder}." ...") if @filesToArchive;
			for my $remoteFile (@filesToArchive) {
				if ($remoteFile =~ /\*/) { # wenn ein glob character enthalten, dann mehrere Files ins Archiv bewegen !
					$logger->debug("moving $remoteFile to ".$FTP->{archiveFolder}." ...");
					my @remoteFiles = $ftp->glob($remoteFile, names_only => 1);
					for my $specFile (@remoteFiles) {
						# $specFile ist trotz names_only leider als relativer pfad zum aktuellen ordner ($FTP->{remoteDir})
						my ($specFilePathOnly, $specFileNameOnly) = ($specFile =~ /^(.*\/)(.*?)$/);
						$specFileNameOnly = $specFile if $specFileNameOnly eq "";
						if ($ftp->rename($specFile,$specFilePathOnly.$FTP->{archiveFolder}."/".$archiveTimestamp.".".$specFileNameOnly)) {
							$logger->debug("Sftp: remote-file ".$specFile." archiviert auf ".$specFilePathOnly.$FTP->{archiveFolder}."/".$archiveTimestamp.".".$specFileNameOnly);
						} else {
							my $errmsg = $ftp->error;
							$logger->error("error: can't rename remote-file ".$specFile." to ".$specFilePathOnly."/".$FTP->{archiveFolder}."/".$archiveTimestamp.".".$specFileNameOnly.", reason: ".$errmsg) if $errmsg !~ /No such file or directory/;
							$logger->warn("Sftp error: ".$errmsg) if $errmsg =~ /No such file or directory/;
						}
					}
				} else {
					if ($ftp->rename($remoteFile,$FTP->{archiveFolder}."/".$archiveTimestamp.".".$remoteFile)) {
						$logger->debug("remote-file ".$FTP->{remoteDir}.$remoteFile." archived to ".$FTP->{archiveFolder}."/".$archiveTimestamp.".".$remoteFile);
					} else {
						my $errmsg = $ftp->error;
						$logger->error("error: can't rename remote-file ".$FTP->{remoteDir}.$remoteFile." to ".$FTP->{archiveFolder}."/".$archiveTimestamp.".".$remoteFile.", reason: ".$errmsg) if $errmsg !~ /No such file or directory/;
						$logger->warn("error: ".$errmsg) if $errmsg =~ /No such file or directory/;
					}
				}
			}
		} else {
			$logger->error("can't change into remote-directory ".$FTP->{remoteDir}.", reason: ".$ftp->error);
		}
	} else {
		$logger->error("no ftp connection opened!");
		return 0;
	}
	return 1;
}

# login, creating a new ftp connection
sub login {
	my ($FTP,$execute) = @_;
	my ($user,$pwd);
	my $logger = get_logger();
	if ($RemoteHost ne $FTP->{remoteHost}{$execute->{env}} or !defined($ftp)) {
		$RemoteHost = $FTP->{remoteHost}{$execute->{env}};
		$logger->info("connecting to [".$RemoteHost."]");
		undef $ftp if defined($ftp); # close ftp connection if open.
	} else {
		$logger->debug("ftp connection already open, using $RemoteHost");
		return 1;
	}
	(!$RemoteHost) and do {
		$logger->error('RemoteHost not set in $FTP->{remoteHost}{'.$execute->{env}.'} !');
		return 0;
	};
	my $maxConnectionTries = $FTP->{maxConnectionTries};
	my $plinkInstallationPath = $FTP->{plinkInstallationPath};
	# only for set prefix, take username and password from $FTP->{$FTP->{prefix}}
	if ($FTP->{prefix}) {
		$user = $FTP->{$FTP->{prefix}}{user};
		$pwd = $FTP->{$FTP->{prefix}}{pwd};
	}
	(!$FTP->{user} && !$user) and do {
		$logger->error("user neither set in \$FTP->{user} nor in \$FTP->{".$FTP->{prefix}."}{user} !");
		return 0;
	};
	# for unstable connections, retry connecting max $maxConnectionTries.
	my $connectionTries = 0;
	# for passwords containing chars that can't be passed via shell to ssh_cmd, quote (\"....\>...\")
	$pwd = Win32::ShellQuote::quote_system($pwd) if ($pwd =~ /[()"<>&]/);
	my $debugLevel = $FTP->{FTPdebugLevel};
	my @moreparams;
	push @moreparams, ("-hostkey", $FTP->{hostkey})  if $FTP->{hostkey};
	push @moreparams, ("-i", $FTP->{privKey}) if $FTP->{privKey};
	push @moreparams, ("-v", "") if $debugLevel;
	unlink $_ for glob($execute->{homedir}."/ftperr*.*");
	do {
		$logger->debug("connection try: $connectionTries");
		# separate setting of debug level, additional to "-v" verbose
		$Net::SFTP::Foreign::debug = $debugLevel;
		my $ssherr = File::Temp->new(TEMPLATE => 'ftperrXXXX',DIR => $execute->{homedir},SUFFIX => '.log') or $logger->error("File::Temp->new failed when creating \$ssherr for Net::SFTP::Foreign->new");
		$ftp = Net::SFTP::Foreign->new(
			host => $RemoteHost,
			user => ($user ? $user : $FTP->{user}),
			password => ($pwd ? $pwd : undef),
			port => ($FTP->{port} ? $FTP->{port} : '22'),
			ssh_cmd => $plinkInstallationPath,
			more => \@moreparams,
			stderr_fh => $ssherr
		);
		$connectionTries++;
		$ftp->error and do {
			# after first failure set full debug ...
			if ($connectionTries == 1) {
				$debugLevel = -1;
			} else {
				$debugLevel = $FTP->{FTPdebugLevel};
			}
			$logger->warn("connection failed: ".$ftp->error.", output from Net::SFTP::Foreign:");
			seek($ssherr, 0, 0);
			$logger->warn($_) while (<$ssherr>);
			close($ssherr);
		};
	} until (!$ftp->error or $connectionTries == $maxConnectionTries);
	if ($connectionTries == $maxConnectionTries and $ftp->error) {
		$logger->error("connection finally failed after $maxConnectionTries connection tries: ".$ftp->error);
		undef $ftp;
		return 0;
	}
	$logger->info("login successful, ftp connection established");
	return 1;
}
1;
__END__
=head1 NAME

ETL::Wrap::FTP - wrapper for Net::FTP resp. Net::SFTP::Foreign

=head1 SYNOPSIS

 removeFilesOlderX ()
 fetchFiles ($param)
 writeFiles ($param)
 moveTempFiles ($param)
 archiveFiles ($param)
 login ($param)

=head1 DESCRIPTION

=item removeFilesOlderX: entferne daten vom FTP server, die älter als Datums/Zeitstempel X sind

 Rückgabe: 1 wenn ALLE Dateien erfolgreich entfernt (bricht nicht ab), 0 wenn fehler.

=item fetchFiles: hole Dateien vom FTP server

 $param .. ref auf hash mit funktionsparametern:
 $param->{filesToRetrieve} .. ref auf array mit files, die abzuholen sind. Wenn ein glob (*) enthalten ist, dann ist localDir für download (mget !) zwingend anzugeben
 $param->{filesToRemove} .. ref auf array mit files, die zu löschen sind, das wird nicht hier gemacht, sondern zur Fehlerunterdrückung verwendet.
 $param->{filesToRetrieveOptional} .. ref auf hash mit files, die optional sind (müssen auch in filesToRetrieve sein)
 $param->{localDir} .. lokale(s) Verzeichnis(sse) für simplen Download (ohne weitere Verarbeitung), bei mehreren File(glob)s ist hier ein ref auf Array von Verzeichnissen anzugeben
 $param->{suppressGetError} .. unterdrücken von fehlermeldungen beim get (erwünscht z.b. bei durchgeführten gewollten wiederholungen),
 $param->{filesTypeAssoc} => übergebener ref auf hash mit file => type (aus @insertOrder) assoziation, diese sind zum gruppieren der erhaltenen files pro type in filesRetrieved, damit unerwünschte files herausgefiltert werden können. Wird in processFiles.pl gesetzt!
 $param->{filesRetrieved} => zurückgegebener ref auf hash mit erhaltenen files bei fileglobs pro type. Wird von processFiles.pl verwendet!
 Rückgabe: 1 wenn ALLE Dateien erfolgreich geholt (bricht nicht ab), 0 wenn fehler.

=item writeFiles: schreibe Dateien auf FTP Server

 Die Dateien werden entweder direkt (dontUseTempFile 1) oder als temp.<name> Dateien (dontUseTempFile 0 oder nicht gesetzt).
 Die temp Dateien werden auf dem Server final umbenannt (wenn dontMoveTempImmediately fehlt oder 0),
 wenn dontMoveTempImmediately =1 geschieht das erst in moveTempFiles. Der Zweck der temp Dateien ist eine atomare transaktion für Dateiüberwachungsjobs zu haben!

 $param .. ref auf hash mit funktionsparametern:
 $param->{filesToWrite} .. ref auf array mit files, die zu hinaufzuladen sind. Diese müssen im aktuellen lokalen Verzeichnis vorhanden sein
 Rückgabe: 1 wenn schreiben ALLER Dateien erfolgreich (bricht beim ersten Fehler ab), 0 wenn fehler.

=item moveTempFiles: separates umbenennen der temp Dateien auf FTP Server in finalen namen (atomare transaktion !)

 $param .. ref auf hash mit parametern:
 $param->{filesToWrite} ..  ref auf array mit files, die umzubenennen sind
 Rückgabe: 1 wenn umbenennen ALLER dateien erfolgreich (bricht beim ersten Fehler ab), 0 wenn fehler.

=item archiveFiles: löschen oder archivieren von Dateien auf dem FTP server, die in $param->{filesToRemove} bzw. $param->{filesToArchive} angegeben sind

 $param .. ref auf hash mit funktionsparametern:
 $param->{filesToArchive} .. ref auf array mit files, die zu archivieren sind, wenn ein glob (vorerst nur für SFTP) angegeben ist, dann wird dieser aufgelöst und alle erhaltenen Files separat archviert.
 $param->{filesToRemove} .. ref auf array mit files, die zu löschen sind
 Rückgabe: 1 wenn löschen oder archivieren ALLER Dateien erfolgreich (bricht nicht ab), 0 wenn fehler (ausser "No such file or directory", hier wird nur ein warning generiert).

=item login: log in auf FTP server, liefert handle der ftp verbindung in ref parameter $ftp. 

 Diese Funktion ist für den internen Gebrauch, sie wird von den anderen Funktionien in FTPUtil verwendet.

 $ftp .. Rückgabeparameter: ref auf handle der ftp verbindung
 Rückgabe: 1 wenn login bzw. wechsel auf binary/ascii erfolgreich, 0 wenn fehler.

=head1 COPYRIGHT

Copyright (c) 2022 Roland Kapl

All rights reserved.  This program is free software; you can
redistribute it and/or modify it under the same terms as Perl itself.

The full text of the license can be found in the LICENSE file included
with this module.

=cut