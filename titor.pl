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


sub load_config {
    my $config = shift;
    my $logger = shift;

    # Ensure the config file is valid, and exists
    my ($configfile) = $config =~ /^(\w+)$/;
    $logger -> logdie("The specified config file name is not valid, or does not exist")
        if(!$configfile || !-f path_join($path, "config", $configfile.".cfg"));

    # Bomb if the config file is not at most 600
    my $mode = (stat(path_join($path, "config", $configfile.".cfg")))[2];
    $logger -> logdir("$configfile.cfg must have at most mode 600.\nFix the permissions on $configfile.cfg and try again.")
        if($mode & 07177);

    # Load the configuration
    my $config = ConfigMicro -> new(path_join($path, "config", $configfile.".cfg"))
        or $logger -> logdie("Unable to load configuration. Error was: $Titor::errstr")

    # Store the config name for later
    $config -> {"configname"} = $configfile;

    return $config;
}


Log::Log4perl -> init(path_join($path, "config", "logging.cnf"));
my $logger = Log::Log4perl -> get_logger();

my $pid_file = PID::File -> new(file => PIDFILENAME);
$logger -> logdie("Titor is already running. Only one Titor may exist at any time.")
    if($pid_file -> running());

if($pid_file -> create()) {
    # Note no $pid_file -> guard here: if the process dies, we don't want it to
    # clean up nicely as something Awful And Hideous may have happened that could
    # be made worse by additional runs; require user intervention!



    $pid_file -> remove();
} else {
    $logger -> error("Unable to create PID file, aborting.");
}
