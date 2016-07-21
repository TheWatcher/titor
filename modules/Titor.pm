## @file
# This file contains the implementation of the Titor base class.
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
# This is a base class for Titor modules, providing common
# features - primarily a simple base constructor and error functions.
package Titor;

use parent qw(Exporter::Tiny);
use Text::Sprintf::Named qw(named_sprintf);
use String::ShellQuote;
use v5.14;
use strict;

our @EXPORT_OK = qw(path_join hash_or_hashref array_or_arrayref dehumanise);
our $errstr;

BEGIN {
    $errstr = '';
}


# ============================================================================
#  Constructor and destructor

## @cmethod $ new(%args)
# Create a new Titor object. The following arguments must be specified when
# creating these object or subclasses of it:
#
# - `sshuser`:    the name of the user to use when connecting to the remote.
# - `sshhost`:    the hostname or IP address of the remote system.
# - `remotepath`: the location on the remote system where backups should go.
# - `logger`:     a logger handle to log operations through.
#
# @param args A hash of key value pairs to initialise the object with.
# @return A new Titor object, or undef if a problem occured.
sub new {
    my $invocant = shift;
    my $class    = ref($invocant) || $invocant;
    my $self     = { sshuser     => undef, # required
                     sshhost     => undef, # required
                     remotepath  => undef, # required

                     sshbase     => '/usr/bin/ssh %(user)s@%(host)s "%(command)s" 2>&1',

                     remotemkdir => '/bin/mkdir -p %(path)s',
                     remotespace => '/bin/df -k --output=avail %(path)s',
                     remoteused  => '/bin/du -ks %(path)s',
                     remoterm    => '/bin/rm -rf %(path)s',
                     remotemv    => '/bin/mv %(source)s %(dest)s',

                     dateformat  => '%Y%m%d-%H%M',

                     margin      => 1048576, # 1GB margin in KB

                     errstr      => '',
                     @_ };

    # Verify required arguments are present
    if(!$self -> {"minimal"}) {
        return set_error("No remote backup path base specified") unless($self -> {"remotepath"});
        return set_error("No remote ssh user specified") unless($self -> {"sshuser"});
        return set_error("No remote ssh host specified") unless($self -> {"sshhost"});
        return set_error("No logger object specified") unless($self -> {"logger"});
    }

    return bless $self, $class;
}


# ============================================================================
#  File and path related functions

## @fn $ path_join(@fragments)
# Take an array of path fragments and concatenate them together. This will
# concatenate the list of path fragments provided using '/' as the path
# delimiter (this is not as platform specific as might be imagined: windows
# will accept / delimited paths). The resuling string is trimmed so that it
# <b>does not</b> end in /, but nothing is done to ensure that the string
# returned actually contains a valid path.
#
# @param fragments An array of path fragments to join together. Items in the
#                  array that are undef or "" are skipped.
# @return A string containing the path fragments joined with forward slashes.
sub path_join {
    my @fragments = @_;
    my $leadslash;

    # strip leading and trailing slashes from fragments
    my @parts;
    foreach my $bit (@fragments) {
        # Skip empty fragments.
        next if(!defined($bit) || $bit eq "");

        # Determine whether the first real path has a leading slash.
        $leadslash = $bit =~ m|^/| unless(defined($leadslash));

        # Remove leading and trailing slashes
        $bit =~ s|^/*||; $bit =~ s|/*$||;

        # If the fragment was nothing more than slashes, ignore it
        next unless($bit);

        # Store for joining
        push(@parts, $bit);
    }

    # Join the path, possibly including a leading slash if needed
    return ($leadslash ? "/" : "").join("/", @parts);
}


# ============================================================================
#  Protected functions - remote commands

## @method protected @ _ssh_cmd($cmd)
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

    my $sshcmd = named_sprintf($self -> {"sshbase"}, user    => $self -> {"sshuser"},
                                                     host    => $self -> {"sshhost"},
                                                     command => $cmd);

    $self -> {"logger"} -> info("Running '$cmd' on remote system...");
    my $res = `$sshcmd`;

    return (${^CHILD_ERROR_NATIVE}, $res);
}


## @method protected $ _remote_mkpath($path)
# Ensure that the specified path exists on the remote system.
#
# @param path The path to create if it does not already exist.
# @return True on success, undef on error.
sub _remote_mkpath {
    my $self = shift;
    my $path = shift;

    $self -> clear_error();

    my $cmd = named_sprintf($self -> {"remotemkdir"}, path => $path);

    my ($status, $msg) = $self -> _ssh_cmd($cmd);
    return $self -> self_error("Remote mkdir failed: '$msg'")
        if($status);

    return 1;
}


## @method protected $ _remote_space($path)
# Determine how much space is available in the specified remote path.
#
# @param path The path to determine the remaining space on.
# @return The amount of space available on the specified path in KB.
sub _remote_space {
    my $self = shift;
    my $path = shift;

    $self -> clear_error();

    my $cmd = named_sprintf($self -> {"remotespace"}, path => $path);

    my ($status, $msg) = $self -> _ssh_cmd($cmd);
    return $self -> self_error("Remote df failed: '$msg'")
        if($status);

    my ($size) = $msg =~ /Avail\s+(\d+)/;
    return $self -> self_error("Unable to parse available space from result '$msg'")
        unless(defined($size));

    return $size;
}


## @method protected $ _remote_used($path)
# Determine how much space has been used in the specified remote path.
#
# @param path The path to determine the used space for.
# @return The amount of space used on the specified path in KB.
sub _remote_used {
    my $self = shift;
    my $path = shift;

    $self -> clear_error();

    my $cmd = named_sprintf($self -> {"remoteused"}, path => $path);

    my ($status, $msg) = $self -> _ssh_cmd($cmd);
    return $self -> self_error("Remote du failed: '$msg'")
        if($status);

    my ($size) = $msg =~ /^(\d+)/;
    return $self -> self_error("Unable to parse available space from result '$msg'")
        unless(defined($size));

    return $size;
}


## @method protected $ _remote_delete($base, $delete)
# Given a base directory and a list of directories inside it, remove the specified
# directories.
#
# @param base   The base directory containing the directories to delete
# @param delete A reference to an array of directories to delete
# @return true on success, undef on error.
sub _remote_delete {
    my $self   = shift;
    my $base   = shift;
    my $delete = shift;

    $self -> clear_error();

    # build the paths to delete
    my @fullpaths = map { path_join($base, $_); } @{$delete};
    my $allpaths  = join(' ', @fullpaths);

    my $cmd;
    if(lc($self -> {"cleanup_type"}) eq "move" && $self -> {"cleanup_dir"}) {
        # Make the outpath relative to base, unless it's already absolute
        my $outpath = $self -> {"cleanup_dir"};
        $outpath = path_join($base, $outpath)
            unless($outpath =~ /^\//);

        $self -> _remote_mkpath($outpath)
            or return undef;

        $cmd = named_sprintf($self -> {"removemv"}, source => $allpaths,
                                                    dest   => $outpath);
    } else {
        $cmd = named_sprintf($self -> {"remoterm"}, path => $allpaths);
    }

    my ($status, $msg) = $self -> _ssh_cmd($cmd);
    return $self -> self_error("Remote rm failed: '$msg'")
        if($status);

    return 1;
}


# ============================================================================
#  Miscellaneous functions

## @fn $ hash_or_hashref(@args)
# Given a list of arguments, if the first argument is a hashref it is returned,
# otherwise if the list length is nonzero and even, the arguments are shoved
# into a hash and a reference to that is returned. If the argument list is
# empty or its length is odd, and empty hashref is returned.
#
# @param args A list of arguments, may either be a hashref or a list of key/value
#             pairs to place into a hash.
# @return A hashref.
sub hash_or_hashref {
    my $len = scalar(@_);
    return {} unless($len);

    # Even number of args? Shove them into a hash and get a ref
    if($len % 2 == 0) {
        return { @_ };

    # First arg is a hashref? Return it
    } elsif(ref($_[0]) eq "HASH") {
        return $_[0];
    }

    # No idea what to do, so give up.
    return {};
}


## @fn $ array_or_arrayref(@args)
# Given a list of arguments, if the first argument is an arrayref it is returned,
# otherwise an arrayref containing the specified arguments is returned.
#
# @param args A list of arguments, may either be an arrayref or a list of values.
# @return An arrayref.
sub array_or_arrayref {
    my @args = @_;
    return [] unless(scalar(@args));

    return $args[0] if(ref($args[0]) eq "ARRAY");
    return \@args;
}


## @fn $ dehumanise($number)
# Given a number (which may end in K, M, G, or KB, MB, GB) return a number that
# is the equivalent in bytes. This is the opposite of the humanise() function,
# in that it can, for example, take a number like 20G and return the value
# 21474836480.
#
# @param number The number to convert to bytes.
# @param The machine-usable version of the number.
sub dehumanise {
    my $number = shift;

    # pull out the number, and the multiplier if present
    my ($num, $multi) = $number =~ /^(\d+(?:\.\d+)?)(K|M|G)?B?$/;

    # If no multiplier is present or recognised, return the number as-is
    if(!$multi) {
        return $num;

    # Otherwise, deal with KB, MB, and GB.
    } elsif($multi eq "K") {
        return $num * 1024;
    } elsif($multi eq "M") {
        return $num * 1048576;
    } elsif($multi eq "G") {
        return $num * 1073741824;
    }
}

# ============================================================================
#  Error functions

## @cmethod private $ set_error($errstr)
# Set the class-wide errstr variable to an error message, and return undef. This
# function supports error reporting in the constructor and other class methods.
#
# @param errstr The error message to store in the class errstr variable.
# @return Always returns undef.
sub set_error {
    $errstr = shift;
    return undef;
}


## @method private $ self_error($errstr)
# Set the object's errstr value to an error message, and return undef. This
# function supports error reporting in various methods throughout the class.
#
# @param errstr The error message to store in the object's errstr.
# @return Always returns undef.
sub self_error {
    my $self = shift;
    $self -> {"errstr"} = shift;

    # Log the error in the database if possible.
    $self -> {"logger"} -> error($self -> {"errstr"})
        if($self -> {"logger"} && $self -> {"errstr"});

    return undef;
}


## @method private void clear_error()
# Clear the object's errstr value. This is a convenience function to help
# make the code a bit cleaner.
sub clear_error {
    my $self = shift;

    $self -> self_error(undef);
}


## @method $ errstr()
# Return the current value set in the object's errstr value. This is a
# convenience function to help make code a little cleaner.
sub errstr {
    my $self = shift;

    return $self -> {"errstr"};
}

1;
