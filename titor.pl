#!/usr/bin/perl -w

## @file
# Main backup script for the titor system. This script loads the modules
# needed to back up databases and directories, and invokes them as needed
# based on a configuration file.
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

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

use Titor qw(path_join dehumanise);
use Titor::Backup;
use Titor::Database;
use Titor::ConfigMicro;
use File::Path qw(make_path);
use Log::Log4perl;
use PID::File;
use Getopt::Long;
use Pod::Usage;

# Where should the PID file go?
use constant PIDPATH     => "/var/run/titor";
use constant PIDFILENAME => "/var/run/titor/titor.pid";


## @fn $ load_config($config, $logger)
# Given the name of a configuration file, attempt to load it from the config
# directory.
#
# @param config  The name of the configuration file to load.
# @param logger  A reference to a Log4perl object.
# @return A reference to a configuration object on success, dies on error.
sub load_config {
    my $config = shift;
    my $logger = shift;

    # Ensure the config file is valid, and exists
    my ($configfile) = $config =~ /^(\w+)$/;
    $logger -> logdie("The specified config file name is not valid, or does not exist")
        if(!$configfile || !-f path_join($path, "config", $configfile.".cfg"));

    # Bomb if the config file is not at most 600
    my $mode = (stat(path_join($path, "config", $configfile.".cfg")))[2];
    $logger -> logdie("$configfile.cfg must have at most mode 600.\nFix the permissions on $configfile.cfg and try again.")
        if($mode & 07177);

    # Load the configuration
    my $confighash = Titor::ConfigMicro -> new(path_join($path, "config", $configfile.".cfg"))
        or $logger -> logdie("Unable to load configuration. Error was: $Titor::errstr");

    # Store the config name for later
    $confighash -> {"configname"} = $configfile;

    return $confighash;
}


## @fn $ process_section($section, $selected)
# Determine whether to process the specified section. If no section
# selection has been made by the user, this will always return true,
# otherwise it will only return true when the section appears in the
# selection list.
#
# @param section  The name of the section to check
# @param selected A reference to a hash of section selections
# @return true if the section should be processed, false otherwise.
sub process_section {
    my $section  = shift;
    my $selected = shift;

    # If no section selection made, always process the section
    return 1 if(!$selected || !scalar(keys(%{$selected})));

    # One or more section selections made, so only return true if
    # the section appears in the selection.
    return $selected -> {$section};
}


my $man        = 0;  # Output the manual?
my $help       = 0;  # Output the summary options
my $configname = 'config'; # Which configuration should be loaded?
my @sections   = (); # Constrain to specific sections?

# Turn on bundling
Getopt::Long::Configure("bundling");

# Process the command line. Explicitly include abbreviations to get around the
# counterintuitive behaviour of Getopt::Long regarding autoabbrev and bundling.
GetOptions('c|config:s'  => \$configname,
           's|section:s' => \@sections,
           'h|help|?'    => \$help,
           'm|man'       => \$man);

# Send back the usage if help has been requested, or there's no files to process.
pod2usage(-verbose => 0) if($help);
pod2usage(-exitstatus => 0, -verbose => 2) if $man;

# Convert any sections to a hash for faster lookup
my %sectmap = map { $_ => 1 } @sections;

Log::Log4perl -> init(path_join($path, "config", "logging.cnf"));
my $logger = Log::Log4perl -> get_logger();

make_path(PIDPATH)
    unless(-d PIDPATH);
my $pid_file = PID::File -> new(file => PIDFILENAME);
$logger -> logdie("Titor is already running. Only one Titor may exist at any time.")
    if($pid_file -> running());

if($pid_file -> create()) {
    # Note no $pid_file -> guard here: if the process dies, we don't want it to
    # clean up nicely as something Awful And Hideous may have happened that could
    # be made worse by additional runs; require user intervention!

    my $config = load_config($configname, $logger);

    $logger -> info("Starting processing of configuration '$configname'");

    $logger -> info("Processing database backups");
    my $database = Titor::Database -> new(logger       => $logger,
                                          sshuser      => $config -> {"server"} -> {"user"},
                                          sshhost      => $config -> {"server"} -> {"hostname"},
                                          remotepath   => $config -> {"server"} -> {"base"},
                                          backup_space => dehumanise($config -> {"server"} -> {"dbsize"}))
        or $logger -> logdie("Database object create failed: ".$Titor::errstr);

    # Back up databases
    foreach my $key (sort(keys(%$config))) {
        # Only process actual database entries...
        next unless($key =~ /^database.\d+$/);
        next unless(process_section($config -> {$key} -> {"name"}, \%sectmap));

        my $dbhandle = $database -> load_module($config -> {$key} -> {"type"} || "mysql",
                                                username  => $config -> {$key} -> {"username"},
                                                password  => $config -> {$key} -> {"password"},
                                                loginpath => $config -> {$key} -> {"loginpath"})
            or $logger -> logdie("Unable to load DB handler: ".$database -> errstr());

        # Do the backup, if a name has been specified only backup that database.
        my $outname;
        if($config -> {$key} -> {"dbname"}) {
            $outname = $dbhandle -> backup_database($config -> {$key} -> {"dbname"}, $config -> {"client"} -> {"tmpdir"})
        } else {
            $outname = $dbhandle -> backup_all($config -> {$key} -> {"name"}, $config -> {"client"} -> {"tmpdir"});
        }

        $logger -> logdie("Unable to back up database: ".$dbhandle -> errstr())
            unless($outname);

        $database -> backup($outname, $config -> {$key} -> {"name"})
            or $logger -> logdie("Database backup failed: ".$database -> errstr());
    }

    # Back up directories
    my $backup = Titor::Backup -> new(logger       => $logger,
                                      sshuser      => $config -> {"server"} -> {"user"},
                                      sshhost      => $config -> {"server"} -> {"hostname"},
                                      remotepath   => $config -> {"server"} -> {"base"},
                                      full_count   => $config -> {"server"} -> {"full_count"},
                                      inc_count    => $config -> {"server"} -> {"inc_count"},
                                      cleanup_type => $config -> {"server"} -> {"cleanup_type"} // "delete",
                                      cleanup_dir  => $config -> {"server"} -> {"cleanup_dir"},
                                      margin       => dehumanise($config -> {"server"} -> {"margin"}))
        or $logger -> logdie("Backup object create failed: ".$Titor::errstr);

    foreach my $key (sort(keys(%$config))) {
        # Only process directory entries...
        next unless($key =~ /^directory.\d+$/);
        next unless(process_section($config -> {$key} -> {"name"}, \%sectmap));

        $backup -> backup($config -> {$key})
            or $logger -> logdie("Backup failed: ".$backup -> errstr());
    }

    $logger -> info("Completed processing of configuration '$configname'");

    $pid_file -> remove();
} else {
    $logger -> error("Unable to create PID file, aborting.");
}

# THE END!
__END__

=head1 NAME

titor.pl - Remote incremental backup system.

=head1 SYNOPSIS

titor.pl [OPTIONS]

 Options:
    -h, -?, --help           Show a brief help message.
    -m, --man                Show full documentation.
    -c, --config             Name of the configuration to use.
    -s, --section            Constrain processing to one or more sections.

=head1 OPTIONS

=over 8
