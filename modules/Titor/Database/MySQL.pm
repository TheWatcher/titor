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
# - `dblist`:      the command to run to get a database list, with a placeholder {login}
#                  for auth information. The result of this should be a list of
#                  database names, one per line.
# - `backup`:      the mysqlbackup command, with placeholders {database} and {output}
#                  for the database name and output name, and {login} for the
#                  login credentials section. This should result in an empty string
#                  on success; any text generated will be treated as an error message.
#
# @param args A hash of key value pairs to initialise the object with.
# @return A new MySQL object, or undef if a problem occured.
sub new {
    my $invocant = shift;
    my $class    = ref($invocant) || $invocant;
    my $filename = shift;
    my $self     = $class -> SUPER::new(username    => undef,
                                        password    => undef,
                                        loginpath   => undef,
                                        dblist      => "/usr/bin/mysql -Bs {login} -e 'SHOW DATABASES;'",

                                        # NOTE: the placement of the 2>&1 is not a mistake! We want to discard stdout,
                                        # and replace it with stderr.
                                        backup      => "/usr/bin/mysqldump {login} -Q -C -E -a -e 2>&1 > {dbname} | /usr/bin/7z a -si {output} 2>&1 > /dev/null",

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


sub backup_database {
    my $self   = shift;
    my $name   = shift;
    my $outdir = shift;



}

1;
