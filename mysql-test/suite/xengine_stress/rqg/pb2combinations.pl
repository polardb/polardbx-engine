# Copyright (C) 2008-2009 Sun Microsystems, Inc. All rights reserved.
# Use is subject to license terms.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301
# USA

use strict;
use Cwd;
use File::Basename;
use POSIX;
use Sys::Hostname;

my ($basedir, $vardir, $tree, $test) = @ARGV;

print("==================== Starting $0 ====================\n");
# Print MTR-style output saying which test suite/mode this is for PB2 reporting.
# So far we only support running one test at a time.
print("##############################################################################\n");
print("# $test\n");
print("##############################################################################\n");

# Autoflush output buffers (needed when using POSIX::_exit())
$| = 1;

# Working dir.
chdir('randgen');
my $cwd = cwd();

#
# Check OS, for later convenience. Windows and Unix/Linux are too different.
#
my $windowsOS;    # undefined if not Windows
if (
	($^O eq 'MSWin32') ||
	($^O eq 'MSWin64')
) {
	$windowsOS = 'true';
}

# Location of grammars and other test configuration files.
# Will use env variable RQG_CONF if set.
# Default is currently "conf" while using legacy setup.
# If not absolute path, it is relative to cwd at run time, which is the randgen directory.
my $conf = $ENV{RQG_CONF};
$conf = 'conf' if not defined $conf;

print("***** Information on the host system: *****\n");
print(" - Local time  : ".localtime()."\n");
print(" - Hostname    : ".hostname()."\n");
print(" - PID         : $$\n");
print(" - Working dir : ".cwd()."\n");
print(" - PATH        : ".$ENV{PATH}."\n");
print(" - Script arguments:\n");
print("       basedir = $basedir\n");
print("       vardir  = $vardir\n");
print("       tree    = $tree\n");
print("       test    = $test\n");
print("\n");
print("***** Information on Random Query Generator version (bzr): *****\n");
system("bzr info");
system("bzr version-info");
print("\n");

mkdir($vardir);

my $command;

# setting number of trials to 1 until we have more stable runs and proper output handling.

if ($test =~ m{falcon_combinations_simple}io ) {
	$command = '
		--grammar='.$conf.'/combinations.yy
		--gendata='.$conf.'/combinations.zz
		--config='.$conf.'/falcon_simple.cc
		--duration=900
		--trials=1
		--seed=time
	';
} elsif ($test =~ m{falcon_combinations_transactions}io ) {
	$command = '
		--grammar='.$conf.'/transactions-flat.yy
		--gendata='.$conf.'/transactions.zz
		--config='.$conf.'/falcon_simple.cc
		--duration=900
		--trials=1
		--seed=time
	';
} elsif ($test =~ m{innodb_combinations_simple}io ) {
	$command = '
		--grammar='.$conf.'/combinations.yy
		--gendata='.$conf.'/combinations.zz
		--config='.$conf.'/innodb_simple.cc
		--mysqld=--innodb
		--duration=1800
		--trials=1
		--seed=time
	';
} elsif ($test =~ m{innodb_combinations_stress}io ) {
	$command = '
		--grammar='.$conf.'/engine_stress.yy
		--gendata='.$conf.'/engine_stress.zz
		--config='.$conf.'/innodb_simple.cc
		--mysqld=--innodb
		--duration=600
		--trials=1
		--seed=time
	';
} elsif ($test =~ m{falcon_combinations_varchar}io ) {
	$command = '
		--grammar='.$conf.'/varchar.yy
		--gendata='.$conf.'/varchar.zz
		--config='.$conf.'/falcon_varchar.cc
		--duration=900
		--trials=1
		--seed=time
	';
} else {
	die("unknown combinations test $test");
}

# Assuming Unix for now (using tail).

$command = "perl combinations.pl --basedir=\"$basedir\" --vardir=\"$vardir\" ".$command;
# redirect output to log file to avoid sending huge amount of output to PB2
my $log_file = $vardir.'/pb2comb_'.$test.'.out';
$command = $command." > $log_file 2>&1";
$command =~ s{[\r\n\t]}{ }sgio;

print localtime()." [$$] Executing command: $command\n";
my $command_result = system($command);
# shift result code to the right to obtain the code returned from the called script
my $command_result_shifted = ($command_result >> 8);
print localtime()." [$$] combinations.pl exited with exit status ".$command_result_shifted."\n";


# Report test result in an MTR fashion so that PB2 will see it and add to
# xref database etc.
# Format: TESTSUITE.TESTCASE 'TESTMODE' [ RESULT ]
# Example: ndb.ndb_dd_alter 'InnoDB plugin'     [ fail ]
# Not using TESTMODE for now.
my $test_suite_name = 'serverqa';
my $full_test_name = $test_suite_name.'.'.$test;
# keep test statuses more or less vertically aligned (if more than one)
while (length $full_test_name < 40)
{
	$full_test_name = $full_test_name.' ';
}

if ($command_result_shifted > 0) {
	# test failed
	print("------------------------------------------------------------------------\n");
	print($full_test_name." [ fail ]\n");
	print("----->  See below for failure details...\n");
} else {
	print($full_test_name." [ pass ]\n");
}
# Print only parts of the output if it is "too large" for PB2.
# This is hopefully just a temporary hack solution...
# Caveats: If the file is shorter than 201 lines, all the output will be sent to std out.
#          If the file is longer than 200 lines, only the first and last parts of the output
#          will be sent to std out.
#          Using 'wc', 'head' and 'tail', so probably won't work on windows (unless required gnu utils are installed)
#          Hanged proceses not especially handled.
#          etc.
my $log_lines = `wc -l < $log_file`;	# number of lines in the log file
if ($log_lines <= 200) {
	# log has 200 lines or less. Display the entire log.
	print("----->  Test log will now be displayed...\n\n");
	open LOGFILE, $log_file or warn "***Failed to open log file [$log_file]";
	print while(<LOGFILE>);
	close LOGFILE;
} elsif ($log_lines > 200) {
	# the log has more than 200 lines. Display the first and last 100 lines.
	my $lines = 100;
	print("----->  Printing first $lines and last $lines lines from test output of $log_lines lines...\n");
	print('----->  See log file '.basename($log_file)." for full output.\n\n");
	system("head -$lines $log_file");
	print("\n.\n.\n.\n.\n.\n(...)\n.\n.\n.\n.\n.\n\n"); # something to visually separate the head and the tail
	system("tail -$lines $log_file");
} else {
	# something went wrong. wc did not work?
	warn("***ERROR during log processing. wc -l did not work? (\$log_lines=$log_lines)\n");
}

# Kill remaining mysqld processes.
# Assuming only one test run going on at the same time, and that all mysqld
# processes are ours.
print("Checking for remaining mysqld processes...\n");
if ($windowsOS) {
	# assumes MS Sysinternals PsTools is installed in C:\bin
	# If you need to run pslist or pskill as non-Admin user, some permission
	# adjustments may be needed. See:
	#   http://blogs.technet.com/markrussinovich/archive/2007/07/09/1449341.aspx
	if (system('C:\bin\pslist mysqld') == 0) {
		print(" ^--- Found running mysqld process(es), to be killed if possible.\n");
		system('C:\bin\pskill mysqld > '.$vardir.'/pskill_mysqld.out 2>&1');
		system('C:\bin\pskill mysqld-nt > '.$vardir.'/pskill_mysqld-nt.out 2>&1');
	} else { print("  None found.\n"); }

} else {
	# Unix/Linux.
	# Avoid "bad argument count" messages from kill by checking if process exists first.
	if (system("pgrep mysqld") == 0) {
		print(" ^--- Found running mysqld process(es), to be killed if possible.\n");
		system("pgrep mysqld | xargs kill -15"); # "soft" kill
		sleep(5);
		if (system("pgrep mysqld > /dev/null") == 0) {
			# process is still around...
			system("pgrep mysqld | xargs kill -9"); # "hard" kill
		}
	} else { print("  None found.\n"); }
}

print localtime()." [$$] $0 will exit with exit status ".$command_result_shifted."\n";
POSIX::_exit ($command_result_shifted);
