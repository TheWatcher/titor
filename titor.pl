#!/usr/bin/perl -w

use strict;
use v5.14;

use FindBin;             # Work out where we are
my $path;
BEGIN {
    $ENV{"PATH"} = "/bin:/usr/bin"; # safe path.

    # $FindBin::Bin is tainted by default, so we may need to fix that
    # NOTE: This may be a potential security risk, but the chances
    # are honestly pretty low...
    if ($FindBin::Bin =~ /(.*)/) {
        $path = $1;
    }
}
use lib "$path/modules"; # Add the script path for module loading

use Titor qw(path_join);
use Titor::Backup;
use Titor::Database;
use Titor::ConfigMicro;
use Log::Log4perl;
use PID::File;

use constant PIDFILENAME => "/var/run/titor";

Log::Log4perl -> init(path_join($path, "config", "logging.cnf"));
my $logger = Log::Log4perl -> get_logger();

my $pid_file = PID::File -> new(file => PIDFILENAME);
$logger -> logdie("Titor is already running. Only one Titor may exist at any time.")
    if($pid_file -> running);
