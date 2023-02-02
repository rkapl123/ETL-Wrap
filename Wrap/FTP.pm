package ETL::Wrap::FTP;

use strict;
use Net::SFTP::Foreign; use Net::SFTP::Foreign::Constants qw( SFTP_ERR_LOCAL_UTIME_FAILED ); 
use Fcntl ':mode'; # for S_ISREG check in removeFilesOlderX
use Log::Log4perl qw(get_logger); use File::Temp; use Time::Local; use Time::localtime; use Exporter; use Data::Dumper;
use ETL::Wrap::DateUtil;
# for passwords that contain <#|>% we have to use shell quoting on windows (special "use" to make this optional on non-win environments)
BEGIN {
	if ($^O =~ /MSWin/) {require Win32::ShellQuote; Win32::ShellQuote->import();}
}

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
										$logger->trace("file: ".$_[1]->{filename}.",mtime: ".$attr->mtime.",mtimeToKeep: ".$mtimeToKeep) if $logger->is_trace;
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
	$queue_size = 1 if !$queue_size; # queue_size bigger 1 causes often connection issues
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
				$logger->trace(Dumper($attr)) if $logger->is_trace;
				$logger->debug("get file $remoteFile");
				if (!$ftp->get($remoteFile, $localFile, queue_size => $queue_size)) { # copy_time => 0
					if (!$param->{fileToRetrieveOptional} and !$FTP->{fileToRemove}) { # ignore errors for a file that was either removed or is optional 
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
				$logger->info("uploading file $localFile, doSetStat: $doSetStat");
				if (!$ftp->put($localFile, $localFile, late_set_perm => 1, copy_perm => $doSetStat, copy_time => $doSetStat)) {
					$logger->error("error: can't upload local file ".$FTP->{remoteDir}.$localFile.", reason: ".$ftp->error);
					return 0;
				}
			} else {
				# safe method for uploading in case a monitor "listens": upload temp file, then rename remotely to final name
				# first rename to temp... locally
				rename $localFile, "temp.".$localFile or $logger->error("error: can't rename local file ".$localFile." to temp.".$localFile.", reason: ".$!) ;
				$logger->info("uploading file temp.$localFile, doSetStat: $doSetStat");
				if (!$ftp->put("temp.".$localFile, "temp.".$localFile)) {
					$logger->error("error: can't upload local temp file to ".$FTP->{remoteDir}."temp.".$localFile.", reason: ".$ftp->error);
					return 0;
				}
				# then name back again remotely
				if (!$FTP->{dontMoveTempImmediately}) {
					$logger->debug("Sftp: remote rename temp file temp.$localFile auf $localFile ...");
					if ($ftp->rename("temp.".$localFile,$localFile)) {
						$logger->debug("Sftp: temporary file ".$FTP->{remoteDir}."temp.".$localFile." renamed to ".$localFile);
					} else {
						my $errmsg = $ftp->error;
						$logger->error("error: can't rename remote-file ".$FTP->{remoteDir}."temp.".$localFile." to ".$localFile.", reason: ".$errmsg) ;
					}
				}
				# last rename temp locally as well for further processing
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
					$logger->error("error: can't remove remote-file ".$FTP->{remoteDir}."/".$remoteFile.", reason: ".$errmsg) if $errmsg !~ /no such file or directory/i;
					$logger->warn("error: ".$errmsg) if $errmsg =~ /no such file or directory/i;
				}
			}
			$logger->info("archiving files @filesToArchive to ".$FTP->{archiveFolder}." ...") if @filesToArchive;
			for my $remoteFile (@filesToArchive) {
				if ($remoteFile =~ /\*/) { # if glob character contained, then move multiple files
					$logger->debug("moving $remoteFile to ".$FTP->{archiveFolder});
					my @remoteFiles = $ftp->glob($remoteFile, names_only => 1);
					for my $specFile (@remoteFiles) {
						# $specFile is a relative path to current folder ($FTP->{remoteDir}, names_only => 1 doesn't help here)
						my ($specFilePathOnly, $specFileNameOnly) = ($specFile =~ /^(.*\/)(.*?)$/);
						$specFileNameOnly = $specFile if $specFileNameOnly eq "";
						if ($ftp->rename($specFile,$specFilePathOnly.$FTP->{archiveFolder}."/".$archiveTimestamp.".".$specFileNameOnly)) {
							$logger->debug("remote-file ".$specFile." archived to ".$specFilePathOnly.$FTP->{archiveFolder}."/".$archiveTimestamp.".".$specFileNameOnly);
						} else {
							my $errmsg = $ftp->error;
							$logger->error("error: can't rename remote-file ".$specFile." to ".$specFilePathOnly."/".$FTP->{archiveFolder}."/".$archiveTimestamp.".".$specFileNameOnly.", reason: ".$errmsg) if $errmsg !~ /No such file or directory/;
							$logger->warn("error: ".$errmsg) if $errmsg =~ /No such file or directory/;
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
	my ($FTP,$execute,$user,$pwd) = @_;
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
		$logger->error('remote host not set in $FTP->{remoteHost}{'.$execute->{env}.'} !');
		return 0;
	};
	my $maxConnectionTries = $FTP->{maxConnectionTries};
	my $plinkInstallationPath = $FTP->{plinkInstallationPath};

	# for unstable connections, retry connecting max $maxConnectionTries.
	my $connectionTries = 0;
	# quote passwords containing chars that can't be passed via windows shell to ssh_cmd (\"....\>...\")
	if ($^O =~ /MSWin/) {
		$pwd = Win32::ShellQuote::quote_system($pwd) if ($pwd =~ /[()"<>&]/);
	}
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

ETL::Wrap::FTP - wrapper for Net::SFTP::Foreign

=head1 SYNOPSIS

 removeFilesOlderX ()
 fetchFiles ($param)
 writeFiles ($param)
 moveTempFiles ($param)
 archiveFiles ($param)
 login ($param)

=head1 DESCRIPTION

=item removeFilesOlderX: remove files on FTP server being older than Date/Timestamp X (given in day/mon/year in remove => {removeFolders => ["",""], day=>, mon=>, year=>1})

 returns 1 if ALL files were removed successfully, 0 on error (doesn't exit early)

=item fetchFiles: get files from FTP server

 $param .. ref to hash with function parameters:
 $param->{fileToRetrieve} .. file to retrieve. if a glob (*) is contained, then multiple files are retrieved
 $param->{fileToRetrieveOptional} .. flag that file is optional

additionally following parameters from $FTP and $execute are important
 $execute->{retrievedFiles} .. returned array with retrieved file (or files if glob was given)
 $execute->{firstRunSuccess} .. used to suppress fetching errors (if first run was already successful)
 $FTP->{queue_size} .. 
 $FTP->{remoteDir} .. root remote Directory on FTP
 $FTP->{path} .. path of folder of file below remoteDir
 $FTP->{localDir} .. alternative storage path, if not given then files are stored to
 $execute->{homedir} .. standard storage path
 $FTP->{fileToRemove} .. ignore errors for a file that was either removed or is optional 
 $FTP->{dontDoUtime} .. 


 returns 1 if ALL files were fetched successfully, 0 on error (doesn't exit early)

=item writeFiles: writes files to FTP server

 the files are written either directly ($FTP->{dontUseTempFile} 1) or as temp.<name> files ($FTP->{dontUseTempFile} 0 or not set).
 those temp files are immediately renamed on the server (if $FTP->{dontMoveTempImmediately} 0 or not set),
 when $FTP->{dontMoveTempImmediately} =1 then this happens in moveTempFiles. This is needed to have an atomic transaction for file monitoring jobs on the FTP site!

 $param .. ref to hash with function parameters:
 $param->{filesToWrite} .. ref to array with files to upload. these have to exist in local folder

 returns 1 if ALL files were written successfully, 0 on error (exits on first error !)

=item moveTempFiles: separately rename temp files on FTP Server to final names (atomic transaction !)

 $param .. ref to hash with function parameters:
 $param->{filesToWrite} ..  ref to array with files to rename from temp to final
 
 returns 1 if ALL files were renamed successfully, 0 on error (exits on first error !)

=item archiveFiles: delete or archive files on FTP server, given in $param->{filesToRemove} or $param->{filesToArchive}

 $param .. ref to hash with function parameters:
 $param->{filesToArchive} .. ref to array with files to be archived if a glob is given, it is being resolved and all retrieved files are archived separately
 $param->{filesToRemove} .. ref to array with files to be deleted

 returns 1 if ALL files were deleted/archived successfully, 0 on error (doesn't exit early), except for "No such file or directory" errors, only warning is logged here

=item login: log in to FTP server, stores the handle of the ftp connection

 returns 1 if loging was successful, 0 on error

=head1 COPYRIGHT

Copyright (c) 2022 Roland Kapl

All rights reserved.  This program is free software; you can
redistribute it and/or modify it under the same terms as Perl itself.

The full text of the license can be found in the LICENSE file included
with this module.

=cut