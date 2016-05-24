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
# supported arguments that may be provided are:
#
# - `logger`: a logger handle to log operations through.
#
# @param args A hash of key value pairs to initialise the object with.
# @return A new Backup object, or undef if a problem occured.
sub new {
    my $invocant = shift;
    my $class    = ref($invocant) || $invocant;
    my $self     = $class -> SUPER::new(sshbase     => '/usr/bin/ssh -p %(port)s %(user)s@%(host)s -e "%(command)s"',

                                        remotespace => '/usr/bin/df -BH %(path)s',
                                        @_)
        or return undef;

    return $self;
}


# ============================================================================
#  Interface





1;