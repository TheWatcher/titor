## @file
# This file contains the code to support the remote backup of databases.
#
# @author  Chris Page &lt;chris@starforge.co.uk&gt;
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see http://www.gnu.org/licenses/.

## @class
# A class to handle incremental, remote backups using rsync and its
# --compare-dest facilities.
#
# This class is based around the functionality that was present in the
# tardis backup system, except that it makes two major departures
# from that system's behaviour:
#
# - it does not present incremental backups as 'full' backups at the
#   filesystem level: no hardlinking trickery is performed, incremental
#   backups contain no files or directories other than the ones changed
#   since the last full or incremental backup. This will hopefully
#   reduce filesystem overhead, making operations faster and more robust.
# - it does not expect the remote filesystem to be in an image file,
#   relying on the limiting performed by the filesystem itself to
#   control backup size. Instead this will self-limit the number of
#   incremental and full backups based on explicitly specified limits.
#
# Both of these changes are intended to make the backup system more
# robust in general use, make filesystem operations faster, and greatly
# simplify the backup process; the only major cost in features this
# introduces is the fact that incremental backups will no longer act
# as system snapshots, and doing a full restore from a given backup
# point will be slightly more complex.
package Titor::Backup;

# How this thing works
# ====================
# This is the general backup process as implemented in the backup() function:
#
# - if there are no backups
#     - make a full backup
# - otherwise
#     - get the newest full backup name
#     - if there are no incrementals for the full backup or increment count < max count
#         - make an incremental for the backup
#     - otherwise the required number of incrementals have been made
#         - if the full backup count is less than the max
#             - make a new full backup
#         - otherwise we've got the max number of full backups so
#             - get the name of the oldest full
#             - rename the oldest full backup to now
#             - sync the renamed backup to the latest state (include --delete)
#             - delete all incrementals for that full backup

use parent qw(Titor);
use strict;
use DateTime;
use File::Path qw(make_path remove_tree);
use Text::Sprintf::Named qw(named_sprintf);
use v5.14;

# ============================================================================
#  Constructor

## @cmethod $ new(%args)
# Create a new Backup object to handle remote backup handling. Please see the
# documentation for the Titor::new() function for required arguments. Optional
# arguments are:
#
# - `full_count`:    the number of full backups to make. Must be > 0, defaults to 2.
# - `inc_count`:     the number of incremental backups to make per full backup, defaults to 10.
# - `margin`:        how much space must be left over on the drive after backup.
# - `rsync_verbose`: show the list of copied files after rsync
#
# @param args A hash of key value pairs to initialise the object with.
# @return A new Backup object, or undef if a problem occured.
sub new {
    my $invocant = shift;
    my $class    = ref($invocant) || $invocant;
    my $self     = $class -> SUPER::new(full_count    => 2,
                                        inc_count     => 10,

                                        remotedirs    => '/bin/ls -1 %(path)s',
                                        remotemv      => '/bin/mv %(source)s %(dest)s',

                                        rsyncremote   => '%(user)s@%(host)s:%(path)s',
                                        rsyncdry      => '/usr/bin/rsync -avz --delete %(exclude)s %(include)s %(compare)s --dry-run --stats %(source)s %(dest)s 2>&1',
                                        rsync         => '/usr/bin/rsync -avz --delete %(exclude)s %(include)s %(compare)s --stats %(source)s %(dest)s 2>&1',
                                        rsync_verbose => 0,

                                        names         => { full        => 'full_',
                                                           incremental => 'inc_',
                                        },

                                        @_)
        or return undef;

    # Verify required arguments are present
    return Titor::self_error("Illegal full backup count specified") unless($self -> {"full_count"} && $self -> {"full_count"} > 0);
    return Titor::self_error("Illegal infremental backup count specified") unless($self -> {"inc_count"} && $self -> {"inc_count"} > 0);

    return $self;
}


# ============================================================================
#  Interface

## @method $ backup(%args)
# Perform a backup operation. This uses the specified arguments, along with the
# object-defined paths and commands, to perform a backup of a local directory
# tree to a remote system. Supported arguments are:
#
# - `name`:        a human-readable name for the backup.
# - `remotedir`:   the name of the backup directory (relative to the global base path)
# - `localdir`:    the local directory to back up
# - `exclude`:     a comma-separated list of rsync exclude rules
# - `excludefile`: the full path of a local file containing rsync exclude rules
# - `include`:     a comma-separated list of rsync include rules
# - `includefile`: the full path of a local file containing rsync include rules
#
# In addition, the constructor arguments may be specified here to locally
# override the settings.
#
# @param args A hash, or reference to a hash, of arguments to determine what
#             needs to be backed up.
# @return True on success (or no action needed), undef on error.
sub backup {
    my $self = shift;
    my $args = Titor::hash_or_hashref(@_);

    $self -> clear_error();

    $self -> _merge_settings($args);

    $self -> {"logger"} -> info("Starting backup for ".$args -> {"name"});

    # Fetch the current list, and work out what needs to be done
    my $backups = $self -> _remote_backup_list($args -> {"remotedir"})
        or return $self -> _restore_settings(undef);

    my $ops = $self -> _backup_paths($backups);

    # include/exclude path directives and the compare list
    my ($include, $exclude) = ( $self -> _rsync_cludes("include", $args),
                                $self -> _rsync_cludes("exclude", $args));

    my $compare = $self -> _rsync_compare_dest($ops -> {"comparelist"});

    # Before actually doing anything to the remote, we need to check there will be enough space there
    my $dryremote =  named_sprintf($self -> {"rsyncremote"}, user => $self -> {"sshuser"},
                                                             host => $self -> {"sshhost"},
                                                             path => Titor::path_join($ops -> {"backuppath"}, $ops -> {"rename"} || $ops -> {"backupdir"}));

    # Undef from the size check indicates there isn't space.
    my $space = $self -> _rsync_size_check($exclude, $include, $compare, $args -> {"localdir"}, $ops -> {"backuppath"}, $dryremote);
    return $self -> _restore_settings(undef) unless(defined($space));

    # 0 indicates that there are no changes
    if(!$space) {
        $self -> {"logger"} -> info("No changes made to ".$args -> {"name"}." since last backup; skipping");
        return $self -> _restore_settings(1);
    }

    # If there's a rename required, do it.
    $self -> _remote_rename($ops -> {"backuppath"}, $ops -> {"rename"}, $ops -> {"backupdir"}) or return $self -> _restore_settings(undef)
        if($ops -> {"rename"});

    # Build the actual destination path now
    my $remotepath = Titor::path_join($ops -> {"backuppath"}, $ops -> {"backupdir"});
    my $remote     = named_sprintf($self -> {"rsyncremote"}, user => $self -> {"sshuser"},
                                                             host => $self -> {"sshhost"},
                                                             path => $remotepath);

    # FIRE ZE MISSILES!
    $self -> _rsync($exclude, $include, $compare, $args -> {"localdir"}, $remote)
        or return $self -> _restore_settings(undef);

    # and remove any incremental directories that need removing
    $self -> _remote_delete($ops ->{"backuppath"}, $ops -> {"delete"}, $self -> {"remotepath"}) or return $self -> _restore_settings(undef)
        if(defined($ops -> {"delete"}) && scalar(@{$ops ->{"delete"}}));

    $self -> {"logger"} -> info("Completed backup for ".$args -> {"name"});
    return $self -> _restore_settings(1);
}


# ============================================================================
#  Private functions - path wrangling

## @method private $ _backup_paths($backups)
# Given a reference to a hash of backup information, work out what the next backup
# step should be. This is the core decision-making logic that underpins how the
# whole backup process proceeds over time, choosing when to make full backups,
# when to make incremental backups, and how and when to cycle the backups.
#
# @param backups A reference to a hash of backup directory data, as generated by
#                the fetch_backup_list() function.
# @return A reference to a hash containing paths needed to perform the backup. This
#         hash will contain some or all of the following keys:
#         - `backuppath`:  the directory on the remote system that contains the backup directories.
#         - `backupdir`:   the name of the directory to write a full or incremental backup into.
#         - `comparelist`: if specified, make an incremental backup; this is a reference to a
#                          list of directories to pass to rsync as `--compare-dest` arguments. If not
#                          specified, make a full backup in `backupdir`
#         - `rename`:      if specified, rename this directory to `backupdir` before doing a
#                          backup.
#         - `delete`:      if specified, a reference to an array of backup directories to delete.
sub _backup_paths {
    my $self    = shift;
    my $backups = shift;
    my $now = DateTime -> now() -> strftime($self -> {"dateformat"});

    # if there are no backups, make a full - return the target path, the full name, no compare list, no rename
    if(!scalar(@{$backups -> {"full"}})) {
        return { backuppath => $backups -> {"base"},
                 backupdir  => $self -> _build_backup_name("full", $now),
        };

    } else {
        # newest full backup is the last one in the full list
        my $newfull = $backups -> {"full"} -> [-1];

        # If the number of incrementals is less than the max count, make an incremental
        if(scalar(@{$newfull -> {"incrementals"}}) < $self -> {"inc_count"}) {
            # Need the date part of the full name.
            my $fulldate = $self -> _get_fullbackup_date($newfull -> {"base"});

            return { backuppath  => $backups -> {"base"},
                     backupdir   => $self -> _build_backup_name("incremental", $fulldate, $now),
                     comparelist => [ $newfull -> {"base"}, @{$newfull -> {"incrementals"}} ]
            };

        # Required number of incrementals have been made for the newest full; have enough full
        # backups been made?
        } elsif(scalar(@{$backups -> {"full"}}) < $self -> {"full_count"}) {
            # Make a new full backup - return the target path, the full name, no compare list, no rename
            return { backuppath => $backups -> {"base"},
                     backupdir  => $self -> _build_backup_name("full", $now),
            };

        # Hit maximum counts for incrementals and full backups, do the wrap-around.
        } else {
            # rename the oldest full backup to the new date, delete its incrementals
            return { backuppath => $backups -> {"base"},
                     backupdir  => $self -> _build_backup_name("full", $now),
                     rename     => $backups -> {"full"} -> [0] -> {"base"},
                     delete     => $backups -> {"full"} -> [0] -> {"incrementals"}
            };
        }
    }
}


## @method private $ _build_backup_hash($remote_path, $dirs)
# Given a list of directories, generate a hash of full and incremental backup
# names. This will use the leading part of the name to work out whether the
# directory corresponds to a full backup, an incremental backup, or some file
# or directory we can ignore.
#
# @param remote_path The path on the remote system containing these backups.
# @param dirs        A reference to a list of directory entries.
# @return A reference to a hash containing the full and incremental backup lists.
sub _build_backup_hash {
    my $self        = shift;
    my $remote_path = shift;
    my $dirs        = shift;

    # build separate lists of full and incremental backups for easier lookup
    my $types = { base        => $remote_path,
                  full        => [],
                  incremental => []
    };
    foreach my $dir (@{$dirs}) {
        if($dir =~ /^$self->{names}->{full}/) {
            push(@{$types -> {"full"}}, $dir);
        } elsif($dir =~ /^$self->{names}->{incremental}/) {
            push(@{$types -> {"incremental"}}, $dir);
        } else {
            $self -> {"logger"} -> debug("Ignoring unknown directory entry '$dir'");
        }
    }

    return $self -> _build_incremental_lists($types);
}


## @method private $ _build_incremental_lists($data)
# Given a hash of full and incremental backup lists, work out which incrementals
# belong to which full backups and make full backup-specific lists of
# incremental directories.
#
# @param data A reference to a hash of full and incremental backup lists.
# @return A reference to a hash of full and incremental backup lists, including
#         per-full-backup incremental lists. An example hash would be:
#         {
#             'base' => '/path/to/remote/base',
#             'incremental' => [ 'inc_20160525-1308.20160526-1300',
#                                'inc_20160525-1308.20160527-1305',
#                                'inc_20160525-1308.20160528-1309',
#                                'inc_20160529-1200.20160530-1204',
#                                'inc_20160529-1200.20160531-1209',
#                              ],
#             'full' => [ { 'incrementals' => [ 'inc_20160525-1308.20160526-1300',
#                                               'inc_20160525-1308.20160527-1305',
#                                               'inc_20160525-1308.20160528-1309'
#                                             ],
#                           'base' => 'full_20160525-1308'
#                         },
#                         { 'incrementals' => [ 'inc_20160529-1200.20160530-1204',
#                                               'inc_20160529-1200.20160531-1209'
#                                             ],
#                           'base' => 'full_20160529-1200'
#                         }
#                       ]
#         }
sub _build_incremental_lists {
    my $self = shift;
    my $data = shift;

    foreach my $full (@{$data -> {"full"}}) {
        # We need the full backup date to find incrementals associated with it.
        my $date = $self -> _get_fullbackup_date($full);

        # Note this assumes dates take the form YYYYMMDD-HHMM
        my @incrementals = grep { /$self->{names}->{incremental}$date.\d{8}-\d{4}/ } @{$data -> {"incremental"}};

        # Modify the element in-place.
        $full = { "base" => $full,
                  "incrementals" => \@incrementals };
    }

    return $data;
}


## @method private $ _get_fullbackup_date($name)
# Given a full backup name, parse out the date stored in the name.
#
# @param name The name of the full backup
# @return The date stored in the backup name.
sub _get_fullbackup_date {
    my $self = shift;
    my $name = shift;

    my ($date) = $name =~ /^$self->{names}->{full}(.*)$/;

    # This should not happen.
    $self -> {"logger"} -> logdie("Unable to parse date from '$name'")
        unless($date);

    return $date;
}


## @method private $ _build_backup_name($type, $base, $ext)
# A convenience method to make creating backup names easier. This generates
# either full or incremental backup names based on the type specified and
# the base date and possibly extension date provided.
#
# @param type The type of name to generate, should be 'full' or 'incremental'
# @param base The base date string, should be in the form YYYYMMDD-HHMM
# @param ext  Optional extension date for incremental backups.
# @return A backup name string.
sub _build_backup_name {
    my $self = shift;
    my $type = shift;
    my $base = shift;
    my $ext  = shift;

    my $name = $self -> {"names"} -> {$type};
    $name .= $base if($base);
    $name .= ".".$ext if($ext);

    return $name;
}


# ============================================================================
#  Private functions - remote commands

## @method private $ _remote_backup_list($name)
# Given a backup name, fetch a list of all the currently stored full or incremental
# backups.
#
# @param name The name of the backup directory to look for directories in.
# @return A reference to a hash containing the full base path to the
#         directory list, and full and incremental backup lists, including
#         per-full-backup incremental lists. See _build_incremental_lists()
#         for an example of the sort of hash this generates.
sub _remote_backup_list {
    my $self = shift;
    my $name = shift;

    $self -> clear_error();

    # Check the remote path exists.
    my $remote_path  = Titor::path_join($self -> {"remotepath"}, $name);
    $self -> _remote_mkpath($remote_path)
        or return undef;

    # Invoke the remote list
    my ($code, $res) = $self -> _ssh_cmd(named_sprintf($self -> {"remotedirs"}, path => $remote_path));

    return $self -> self_error("Unable to list remote backups, response was: '$res'")
        if($code); # Successful listing should result in 0

    # Convert to an array for easier processing.
    my @files = split(/^/, $res);
    chomp(@files);

    # It's gone well so far, try to make a hash of it!
    return $self -> _build_backup_hash($remote_path, \@files);
}


# @method private $ _remote_rename($path, $srcdir, $destdir)
# Given a path, and two directory names within that path, attempt to rename
# the source to the destination.
#
# @param path    The remote directory contianing the directories to rename.
# @param srcdir  The source directory name.
# @param destdir The destination directory name.
# @return true on success, undef on error.
sub _remote_rename {
    my $self    = shift;
    my $path    = shift;
    my $srcdir  = shift;
    my $destdir = shift;

    $self -> clear_error();

    my $cmd = named_sprintf($self -> {"remotemv"}, source => Titor::path_join($path, $srcdir),
                                                   dest   => Titor::path_join($path, $destdir));

    my ($status, $msg) = $self -> _ssh_cmd($cmd);
    return $self -> self_error("Remote rename failed: '$msg'")
        if($status || $msg);

    return 1;
}


# ============================================================================
#  Private functions - rsync support

## @method private $ _rsync_cludes($mode, $settings)
# Given a mode, either 'exclude' or 'include' work out whether the settings
# contain include or exclude rules or files, and build rsync command line
# fragments for thise rules.
#
# @param mode     The strint 'include' or 'exclude'.
# @param settings A reference to a hash of backup settings.
# @return A string containing rsync include or exclude directives, or an
#         empty string if the settings do not contain such directives.
sub _rsync_cludes {
    my $self     = shift;
    my $mode     = shift;
    my $settings = shift;

    my $result = "";
    if($settings -> {$mode}) {
        my @cludes = split(/,/, $settings -> {$mode});

        # Build up a series of arguments
        foreach my $rule (@cludes) {
            $result .= " --$mode='$rule'";
        }
    }

    # If the config has an file set, record it.
    my $modefile = $mode."file";
    $result .= " --$mode-from='".$settings -> {$modefile}."'"
        if($settings -> {$modefile} && -f $settings -> {$modefile});

    return $result;
}


## @method private $ _rsync_compare_dest($comparelist)
# Build a list of '--compare-dest' arguments to rsync based on the specified
# array of directories. Not that, if the directories in the compare list do
# not start with '/', they are assumed to be relative to the destination
# and will have '../' prepended.
#
# @param comparelist A reference to an array of directories.
# @return A string containing the --compare-dest arguments
sub _rsync_compare_dest {
    my $self        = shift;
    my $comparelist = shift;

    return "" unless($comparelist && scalar(@{$comparelist}));

    my $result = "";
    foreach my $path (@{$comparelist}) {
        # Make paths relative to the destination unless absolute
        $path = "../$path"
            unless($path =~ /^\//);

        $result .= " --compare-dest='$path'";
    }

    return $result;
}


## @method private % _rsync_size_check($exclude, $include, $compare, $localdir, $remotebase, $remote)
# Determine whether there is enough space on the remote system to perform the
# rsync operation successfully. This does a dry run of the rsync operation, and
# checks that the reported transfer size fits into the available space (with
# some wiggle room)
#
# @param exclude    The exclusion directives to pass to rsync.
# @param include    The inclusion directives for rsync.
# @param compare    Any --compare-dest directives to pass to rsync.
# @param localdir   The source directory for the backup operation.
# @param remotebase The remote backup base directory.
# @param remote     The full rsync destination string.
# @return True if enough space is availanle for the backup, 0 if there is nothing
#         to do (no changes since last backup), under otherwise.
sub _rsync_size_check {
    my $self       = shift;
    my $exclude    = shift;
    my $include    = shift;
    my $compare    = shift;
    my $localdir   = shift;
    my $remotebase = shift;
    my $remote     = shift;

    # Dry run to fetch sizes
    my $drycmd = named_sprintf($self -> {"rsyncdry"}, exclude => $exclude,
                                                      include => $include,
                                                      compare => $compare,
                                                      source  => $localdir,
                                                      dest    => $remote);
    $self -> {"logger"} -> info("Calculating how much data will be transferred.");
    $self -> {"logger"} -> info("Running command: $drycmd");
    my $res = `$drycmd`;
    return $self -> self_error("Rsync dry-run failed: '$res'")
        if(${^CHILD_ERROR_NATIVE});

    # We're really only interested in the number of bytes transferred
    my ($update) = $res =~ /^Total transferred file size: ([\d,]+) bytes$/m;
    return $self -> self_error("Unable to parse transfer size from '$res'")
        unless(defined($update));

    $update =~ s/,//g; # Pesky commas, begone.
    return 0 if(!$update); # Stop here if there's nothing to send.

    $update /= 1024;   # And we want the size in K, not bytes.

    # How much space to we have remotely?
    my $space = $self -> _remote_space($remotebase);
    return undef if(!defined($space));

    return $self -> self_error(sprintf("Insufficient space left on backup system: backup requires %.2fK, %.2fK available", $update, $space))
        if($space < (($self -> {"margin"} / 1024) + $update));

    $self -> {"logger"} -> info(sprintf("Backup requires %.2fK, %.2fK available", $update, $space));

    return 1;
}


## @method private $ _rsync($exclude, $include, $compare, $localdir, $remote)
# Invoke rsync to copy local data to the remote system.
#
# @param exclude    The exclusion directives to pass to rsync.
# @param include    The inclusion directives for rsync.
# @param compare    Any --compare-dest directives to pass to rsync.
# @param localdir   The source directory for the backup operation.
# @param remote     The full rsync destination string.
# @return true on success, undef on error.
sub _rsync {
    my $self       = shift;
    my $exclude    = shift;
    my $include    = shift;
    my $compare    = shift;
    my $localdir   = shift;
    my $remote     = shift;

    my $cmd = named_sprintf($self -> {"rsync"}, exclude => $exclude,
                                                include => $include,
                                                compare => $compare,
                                                source  => $localdir,
                                                dest    => $remote);

    $self -> {"logger"} -> info("Performing backup.");
    $self -> {"logger"} -> info("Running command: $cmd");
    my $res = `$cmd`;
    return $self -> self_error("Rsync failed: '$res'")
        if(${^CHILD_ERROR_NATIVE});

    # Show what's been copied if requested
    print $res if($self -> {"rsync_verbose"});

    my ($update) = $res =~ /^Total transferred file size: ([\d,]+) bytes$/m;
    return $self -> self_error("Unable to parse transfer size from '$res'")
        unless(defined($update));

    $update =~ s/,//g; # Pesky commas, begone.
    $update /= 1024;   # And we want the size in K, not bytes.

    $self -> {"logger"} -> info(sprintf("Backup complete, %.2fKB transferred.", $update));

    return 1;
}


# ============================================================================
#  Private functions - settings support


## @method private void _merge_settings($args)
# Merge any count and margin settings specified in the arguments into the
# object settings. This will back up the current settings before changing
# them so they may be restored with _restore_settings().
#
# @param args A reference to a hash of arguments.
sub _merge_settings {
    my $self = shift;
    my $args = shift;

    # Back up the settings in case we need to restore them later
    $self -> {"backup"} = { full_count    => $self -> {"full_count"},
                            inc_count     => $self -> {"inc_count"},
                            margin        => $self -> {"margin"},
                            rsync_verbose => $self -> {"rsync_verbose"},
    };


    $self -> {"full_count"}    = $args -> {"full_count"}    if($args -> {"full_count"} && $args -> {"full_count"} > 0);
    $self -> {"inc_count"}     = $args -> {"inc_count"}     if($args -> {"inc_count"} && $args -> {"inc_count"} > 0);
    $self -> {"rsync_verbose"} = $args -> {"rsync_verbose"} if(defined($args -> {"rsync_verbose"}));
    $self -> {"margin"}        = Titor::dehumanise($args -> {"margin"})
        if($args -> {"margin"} && $args -> {"margin"} =~ /^\d+/);
}


## @method private $ _restore_settings($retval)
# Restore the settings backed up in _merge_settings().
#
# @param retval The value to return.
# @return The value specified in retval.
sub _restore_settings {
    my $self   = shift;
    my $retval = shift;

    $self -> {"full_count"}    = $self -> {"backup"} -> {"full_count"};
    $self -> {"inc_count"}     = $self -> {"backup"} -> {"inc_count"};
    $self -> {"rsync_verbose"} = $self -> {"backup"} -> {"rsync_verbose"};
    $self -> {"margin"}        = $self -> {"backup"} -> {"margin"};

    return $retval;
}

1;
