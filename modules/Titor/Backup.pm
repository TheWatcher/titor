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
#

# How this thing works
# ====================
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

package Titor::Backup;

use parent qw(Titor);
use strict;
use DateTime;
use File::Path qw(make_path remove_tree);
use Text::Sprintf::Named qw(named_sprintf);
use v5.14;


# ============================================================================
#  Constructor

## @cmethod $ new(%args)
# Create a new Backup object to handle remote backup handling. The
# supported arguments that must be provided are:
#
# - `sshuser`:    the name of the user to use when connecting to the remote.
# - `sshhost`:    the hostname or IP address of the remote system.
# - `remotepath`: the location on the remote system where backups should go.
# - `logger`:     a logger handle to log operations through.
#
# Optional arguments are:
#
# - `sshport`:    the port to connect to ssh through, defaults to 22.
# - `full_count`: the number of full backups to make. Must be > 0, defaults to 2.
# - `inc_count`:  the number of incremental backups to make per full backup, defaults to 10.
#
# @param args A hash of key value pairs to initialise the object with.
# @return A new Backup object, or undef if a problem occured.
sub new {
    my $invocant = shift;
    my $class    = ref($invocant) || $invocant;
    my $self     = $class -> SUPER::new(sshbase     => '/usr/bin/ssh -p %(port)s %(user)s@%(host)s "%(command)s" 2>&1',
                                        sshuser     => undef,
                                        sshport     => 22,
                                        sshhost     => undef,

                                        full_count  => 2,
                                        inc_count   => 10,

                                        remotespace => '/usr/bin/df -BH %(path)s',
                                        remotedirs  => '/bin/ls -1 %(path)s',
                                        remoterm    => '/bin/rm -rf %(path)s',
                                        remotemv    => '/bin/mv %(source)s %(dest)s',

                                        names       => { full        => 'full_',
                                                         incremental => 'inc_',
                                        },
                                        dateformat  => '%Y%m%d-%H%M',

                                        @_)
        or return undef;

    return Titor::self_error("No remote backup path base specified") unless($self -> {"remotepath"});
    return Titor::self_error("No remote ssh user specified") unless($self -> {"sshuser"});
    return Titor::self_error("No remote ssh host specified") unless($self -> {"sshhost"});
    return Titor::self_error("Illegal full backup count specified") unless($self -> {"full_count"} && $self -> {"full_count"} > 0);
    return Titor::self_error("Illegal infremental backup count specified") unless($self -> {"inc_count"} && $self -> {"inc_count"} > 0);

    # Precalculate the ssh command as much as possible.

    return $self;
}


# ============================================================================
#  Interface

sub backup {
    my $self = shift;
    my %args = @_;



}


# ============================================================================
#  Private functions

## @method private $ _fetch_backup_list($name)
# Given a backup name, fetch a list of all the currently stored full or incremental
# backups.
#
# @param name The name of the backup directory to look for directories in.
# @return A reference to a hash containing the full base path to the
#         directory list, and full and incremental backup lists, including
#         per-full-backup incremental lists. See _build_incremental_lists()
#         for an example of the sort of hash this generates.
sub _fetch_backup_list {
    my $self = shift;
    my $name = shift;

    $self -> clear_error();

    # Invoke the remote list
    my $remote_path  = Titor::path_join($self -> {"remotepath"}, $name);
    my ($code, $res) = $self -> _ssh_cmd(named_sprintf($self -> {"remotedirs"}, path => $remote_path));

    return $self -> self_error("Unable to list remote backups, response was: '$res'")
        if($code); # Successful listing should result in 0

    # Convert to an array for easier processing.
    my @files = split(/^/, $res);
    chomp(@files);

    # It's gone well so far, try to make a hash of it!
    return $self -> _build_backup_hash($remote_path, \@files);
}


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


## @method private @ _ssh_cmd($cmd)
# Execute the specified command on the remote host using SSH. This creates
# a connection to the remote host over ssh, and runs the command, returning
# the string result of the operation.
#
# @param cmd The command to run on the remote host.
# @return An array of two values: the first is the exit status of the command
#         (0 indicates both the command and ssh connection were successful,
#         non-zero means an error occurred), the second is a string containing
#         the output of the command (possibly including output from ssh on error)
sub _ssh_cmd {
    my $self = shift;
    my $cmd  = shift;

    my $sshcmd = named_sprintf($self -> {"sshbase"}, port    => $self -> {"sshport"},
                                                     user    => $self -> {"sshuser"},
                                                     host    => $self -> {"sshhost"},
                                                     command => $cmd);

    $self -> {"logger"} -> info("Running '$cmd' on remote system...");
    my $res = `$sshcmd`;

    return (${^CHILD_ERROR_NATIVE}, $res);
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


1;