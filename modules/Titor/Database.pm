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
# Create a new database object to handle dynamic loading of database-specific backup
# modules. The supported arguments that may be provided are:
#
# - `logger`: a logger handle to log operations through.
#
# @param args A hash of key value pairs to initialise the object with.
# @return A new Database object, or undef if a problem occured.
sub new {
    my $invocant = shift;
    my $class    = ref($invocant) || $invocant;
    my $self     = $class -> SUPER::new(@_)
        or return undef;

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

# All subclasses should implement both of these methods.

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

1;
