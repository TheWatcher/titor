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
use parent qw(Exporter::Tiny);

our @EXPORT_OK = qw(path_join hash_or_hashref array_or_arrayref);

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
