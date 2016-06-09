## @file
# This file contains the code to support the listing and dumping
# of databases
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
package Titor::Database;

use parent qw(Titor);
use strict;
use v5.14;
use Module::Load;


# ============================================================================
#  Constructor

## @cmethod $ new(%args)
# Create a new database object to handle dynamic loading of database-specific
# backup modules and common database backup operations. Please see the
# documentation for the Titor::new() function for required arguments. Optional
# arguments are:
#
# - `backup_space`: The amount of space that database backups may occupy,
#                   in KB. Defaults to 2097152 (2GB)
#
# @param args A hash of key value pairs to initialise the object with.
# @return A new Database object, or undef if a problem occured.
sub new {
    my $invocant = shift;
    my $class    = ref($invocant) || $invocant;
    my $self     = $class -> SUPER::new(backup_space => 2097152,

                                        # Lists files only in the specified directory, tab sep columns: timestamp, size, name
                                        remotedirs  => '/usr/bin/find %(path)s -maxdepth 1 -type f -printf "%T@\t%s\t%p\n" | sort',

                                        copycmd => '/usr/bin/scp -q %(source)s %(user)s@%(host)s:%(dest)s',

                                        @_)
        or return undef;

    # Registration of known database backup modules.
    $self -> {"modules"} = { "mysql" => "Titor::Database::MySQL" };

    return $self;
}


# ============================================================================
#  Dynamic loader

## @method $ load_module($name, %args)
# Dynamically load the database backup module named, passing the provided arguments
# to its constructor.
#
# @param name The name of the database backup module to load. This may be either a
#             perl module name, or an alias.
# @param args A hash of arguments to pass to the database backup module constructor.
# @return A reference to a database backup module object on success, undef on error.
sub load_module {
    my $self = shift;
    my $name = shift;
    my %args = @_;

    $args{"logger"} = $self -> {"logger"};

    # Convert name to a module name, unless it is one.
    $name = $self -> {"modules"} -> {$name}
        unless($name =~ /^Titor::Database::/);

    return $self -> self_error("Unable to find implementation module for specified alias.")
        if(!$name);

    # Handle dynamic loading. Note that
    no strict 'refs';
    eval { load $name };
    return $self -> self_error("Unable to load module '$name': $@")
        if($@);

    my $modobj = $name -> new(%args)
        or return $self -> self_error("Unable to create instance of '$name': ".$Titor::errstr);
    use strict;

    return $modobj;
}


# ============================================================================
#  Interface


## @method $ backup($backupfile, $name)
#
# @param backupfile The full name of the local backup file.
# @param name       The remote backup directory name relative to $self -> {"remotepath"}
# @return true on success, undef on error.
sub backup {
    my $self       = shift;
    my $backupfile = shift;
    my $name       = shift;

    $self -> clear_error();

    # Full directory on the remote where the backups should be stored
    my $remote_path  = Titor::path_join($self -> {"remotepath"}, $name);
    $self -> _remote_mkpath($remote_path)
        or return undef;

    my $size = -s $backupfile;
    $self -> self_error("Unable to obtain size for backup file '$backupfile'")
        unless(defined($size));

    $self -> self_error("Specified backup file is empty")
        if(!$size);

    # Ensure there's enough space
    $self -> _remote_check($remote_path, $size / 1024)
        or return undef;

    # Do the copy!
    my $scpcmd = named_sprintf($self -> {"copycmd"}, user   => $self -> {"sshuser"},
                                                     host   => $self -> {"sshhost"},
                                                     source => $backupfile,
                                                     dest   => $remote_path);

    $self -> {"logger"} -> info("Running '$scpcmd' to copy to remote...");
    my $res = `$scpcmd`;
    return $self -> self_error("Unable to copy backup: $res")
        if(${^CHILD_ERROR_NATIVE});

    $self -> {"logger"} -> info("Database backup successfully sent to remote.");

    return 1;
}


# IMPORTANT: All subclasses must implement both of these methods.

# @method $ backup_database($name, $outdir, $now)
# Back up the specified database to a file in the output directory. This will run the
# backup command for the named database, writing the output to a file with the name
# databasename_YYYYMMDD-HHMMSS.sql (the backup command may add other extensions) in the
# output directory specified.
#
# @param name   The name of the database to back up.
# @param outdir The directory to write the database backup file to.
# @param now    Optional reference to a DateTime object to use to timestamp filenames.
# @return the name of the backup file on successful backup, undef on error.
sub backup_database {
    my $self   = shift;

    return $self -> self_error("Call to unimplemented backup_database()");
}


## @method $ backup_all($name, $outdir, $exclude, ...)
# Backup all databases in the system, other than those excluded by default rules
# or explicitly specified. This fetches the list of databases in the system and
# creates a backup of them as a single archive containing one archive per database.
#
# @param name    The base name of the backup archive. This will be appended with the
#                date and time of backup.
# @param outdir  The directory to write the backup archive to.
# @param exclude An array, or reference to an array, of database names to exclude
#                from the backup. This is merged with the internal exclusion list,
#                so you do not need to explicily exclude the information or
#                performance schema tables yourself.
# @return The filename of the backup archive on success, undef on error.
sub backup_all {
    my $self    = shift;

    return $self -> self_error("Call to unimplemented backup_all()");
}


# ============================================================================
#  Private functions - remote work

## @method private $ _remote_check($path, $size)
# Determine whether there is sufficient space on the remote system to store
# a backup with the specified size, potentially deleting old backups to make
# space as needed.
#
# @param path The remote path that should contain the backup.
# @param size The size of the backup file in KB.
# @return true on success, undef on error.
sub _remote_check {
    my $self = shift;
    my $path = shift;
    my $size = shift;

    $self -> clear_error();

    # how much space is the remote using?
    my $used = $self -> _remote_used($path);
    return undef unless(defined($used));

    # Is there enough space for the backup?
    my $remain = $self -> {"backup_space"} - ($used + $self -> {"margin"});
    return 1 if($remain >= $size);

    # not enough space, pull the list of backups so they can be deleted
    my $backups = $self -> _remote_list($path)
        or return undef;

    # work out which backups need to be deleted to free space
    foreach my $backup (@{$backups}) {
        $backup -> {"delete"} = 1;

        $remain += $backup -> {"size"};
        last if($remain >= $size);
    }

    return $self -> self_error("Unable to delete enough backups to make space for new backup")
        unless($remain >= $size);

    # There should be space after deletes, so do them
    foreach my $backup (@{$backups}) {
        last if(!$backup -> {"delete"});

        $self -> _remote_delete($backup -> {"path"})
            or return undef;
    }

    return 1;
}


## @method private $ _remote_list($path)
# Fetch a list of the files in the specified path on the remote system, including
# the time at which the file was last modified, and the size of the file. The
# returned filenames are absolute if `path` is absolute.
#
# @param path The path to fetch the list of files for.
# @return A reference to an array of hashes containing file data, oldest file
#         first.
sub _remote_list {
    my $self = shift;
    my $path = shift;

    $self -> clear_error();

    my $cmd = named_sprintf($self -> {"remotedirs"}, path => $path);

    my ($status, $msg) = $self -> _ssh_cmd($cmd);
    return $self -> self_error("Remote file list failed: '$msg'")
        if($status);

    my @files = ();
    my @rows = split(/^/, $msg);
    foreach my $row (@rows) {
        my ($time, $size, $name) = $row =~ /^(\d+)\.\d+\t(\d+)\t(.*)$/;
        return $self -> self_error("Unable to parse file information from '$row'")
            unless($time && defined($size) && $name);

        push(@files, { time => $time, size => $size, name => $name });
    }

    return \@files;
}

1;
