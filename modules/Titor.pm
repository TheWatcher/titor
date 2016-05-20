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
# Subclasses will generally only need to override the constructor, usually
# chaining it with `$class -> SUPER::new(..., @_);`. If attempting to call
# set_error() in a subclass, remember to use Titor::set_error().
package Titor;

use strict;
use Scalar::Util 'blessed';

our $errstr;

BEGIN {
    $errstr = '';
}


# ============================================================================
#  Constructor and destructor

## @cmethod $ new(%args)
# Create a new Titor object.
#
# @param args A hash of key value pairs to initialise the object with.
# @return A new Titor object, or undef if a problem occured.
sub new {
    my $invocant = shift;
    my $class    = ref($invocant) || $invocant;
    my $self     = {
        errstr => '',
        @_,
    };

    return bless $self, $class;
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
    $self -> {"logger"} -> log("error", 0, undef, $self -> {"errstr"})
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
