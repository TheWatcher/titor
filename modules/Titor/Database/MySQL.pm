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
package Titor::Database::MySQL;

use parent qw(Titor::Database);
use DateTime;
use File::Path qw(make_path remove_tree);
use Text::Sprintf::Named qw(named_sprintf);


## @cmethod $ new(%args)
# Create a new MySQL object. The supported values you can pass as arguments are:
#
# - `username`:    optional username to use for MySQL queries. This user should have
#                  the appropriate permissions to access the database(s) being
#                  backed up. Generally you don't want to set this, use `loginpath`.
# - `password`:    optional password to provide when doing MySQL queries. This will
#                  appear on the command line used to invoke mysql and mysqlbackup
#                  so it should be considered unsafe, and only used if `loginpath`
#                  can not be used.
# - `loginpath`:   the name of the login path to use for authentication, see:
#                  https://dev.mysql.com/doc/mysql-utilities/1.6/en/mysql-utils-intro-connspec-mylogin.cnf.html
#                  This is the recommended option for authentication, as it ensures
#                  that the database credentials do not appear on the command line.
#
# The following arguments are optional, and default to sane values for normal setups:
#
# - `dblist`:      command to run to get a database list, with a placeholder %(login)s
#                  for auth information. The result of this should be a list of
#                  database names, one per line. It *must not* include any other data
#                  (decoration, column header, etc).
# - `backup`:      mysqlbackup command, with placeholders %(database)s and %(output)s
#                  for the database name and output name, and %(login)s for the
#                  login credentials section. The command this invokes should result
#                  in an empty string on success; any text generated will be treated
#                  as an error message.
# - `backupext`:   extension added during backup.
# - `archive`:     command used to archive multiple database backups into one file.
# - `archiveext`:  extension to add to the archive file
#
# @param args A hash of key value pairs to initialise the object with.
# @return A new MySQL object, or undef if a problem occured.
sub new {
    my $invocant = shift;
    my $class    = ref($invocant) || $invocant;
    my $self     = $class -> SUPER::new(username    => undef,
                                        password    => undef,
                                        loginpath   => undef,
                                        dblist      => '/usr/bin/mysql %(login)s -Bs -e "SHOW DATABASES;"',

                                        # NOTE: the placement of the second 2>&1 is not a mistake! We want to discard stdout,
                                        # and replace it with stderr.
                                        backup      => '/usr/bin/mysqldump %(login)s -Q -C -E -a -e 2>&1 %(dbname)s | /usr/bin/7z a -si \'%(output)s\' 2>&1 > /dev/null',
                                        backupext   => '7z',

                                        # There's no point in compressing this - it'll be a tar of 7zip files, nothing
                                        # is going to make that smaller really.
                                        archive     => '/bin/tar -cf %(output)s -C %(workdir)s %(source)s',
                                        archiveext  => 'tar',

                                        # Internal database names to always skip.
                                        exclude     => [ 'information_schema',
                                                         'performance_schema'
                                        ],

                                        @_)
        or return undef;

    # fix up the login variable based on potential auth parameters.
    if($self -> {"loginpath"}) {
        $self -> {"login"} = "--login-path='".$self -> {"loginpath"}."'";
    } elsif($self -> {"username"} && $self -> {"password"}) {
        $self -> {"login"} = "--username='".$self -> {"username"}."' --password='".$self -> {"password"}."'";
    } else {
        return Titor::set_error("No auth credentials specified in Titor::Database::MySQL constructor.");
    }

    return $self;
}


## @method $ backup_database($name, $outdir, $now)
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
    my $name   = shift;
    my $outdir = shift;
    my $now    = shift // DateTime -> now();

    $self -> clear_error();

    my $outname = Titor::path_join($outdir, $name.$now -> strftime("_%Y%m%d-%H%M%S.sql.".$self -> {"backupext"}));

    $self -> {"logger"} -> info("Backing up database '$name' to '$outname'... ");

    my $cmd = named_sprintf($self -> {"backup"}, login => $self -> {"login"},
                                                 dbname => $name,
                                                 output => $outname);

    my $out = `$cmd`;
    return $self -> self_error("Backup of '$name' failed. Output was: '$out'")
        if($out || ${^CHILD_ERROR_NATIVE});

    $self -> {"logger"} -> info("'$name' backup complete.");

    return $outname;
}


## @method $ backup_all($name, $outdir, $exclude, ...)
# Backup all databases in the system, other than those excluded by default rules
# or explicitly specified. This fetches the list of databases in the system and
# creates a backup of them as a single archive containing one archive per database.
#
# @note This does not use the simple -A argument to mysqldump; each database is
#       backed up individually to its own sql file. This is a deliberate decision
#       to make it easier to restore individual databases, and to make it more
#       likely that the backups will restore *at all*.
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
    my $name    = shift;
    my $outdir  = shift;
    my $exclude = Titor::array_or_arrayref(@_);
    my $now     = DateTime -> now();

    $self -> clear_error();

    # extend the exclusion list with any internally-defined exclusions
    push(@{$exclude}, @{$self -> {"exclude"}});

    # Convert to hash for fast lookup
    my %excludedbs = map { $_ => 1 } @{$exclude};

    my $databases = $self -> _get_databases()
        or return undef;

    # process the list of databases, writing the backups into a timestamped dir.
    my $basename = $name.$now -> strftime("_%Y%m%d-%H%M%S");
    my $outpath  = Titor::path_join($outdir, $basename);
    eval { make_path($outpath) };
    return $self -> self_error("Unable to create backup path: $@")
        if($@);

    foreach my $dbname (@{$databases}) {
        next if($excludedbs{$dbname}); # Skip excluded databases.

        $self -> backup_database($dbname, $outpath, $now)
            or return undef;
    }

    # Databases dumped, tar them up
    my $outname = $outpath.".".$self -> {"archiveext"};

    $self -> {"logger"} -> info("Archiving backups as '$outname'...");

    my $cmd = named_sprintf($self -> {"archive"}, output  => $outname,
                                                  workdir => $outdir,
                                                  source  => $basename);
    my $res = `$cmd`;
    return $self -> self_error("Error archiving backups. Output was: '$res'")
        if($res || ${^CHILD_ERROR_NATIVE});

    # Clean up the temporary backup path (see warnings in File::Path for why this
    # isn't done in an exec { }.
    remove_tree($outpath);

    $self -> {"logger"} -> info("Backup archive complete.");

    return $outname;
}


# ============================================================================
#  Private functions

## @method private $ _get_databases()
# Fetch the list of databases defined in the system.
#
# @return An array of database names on success, undef on error.
sub _get_databases {
    my $self = shift;

    $self -> clear_error();

    my $cmd = named_sprintf($self -> {"dblist"}, login => $self -> {"login"});

    my $out = `$cmd`;
    return $self -> self_error("Unable to fetch list of databases. Error was: '$out'")
        if(${^CHILD_ERROR_NATIVE});

    # One database name per line, so split to an array without newlines
    my @databases = split(/^/, $out);
    chomp(@databases);

    return \@databases;
}

1;
