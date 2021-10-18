# Copyright (C) 2008-2010 Sun Microsystems, Inc. All rights reserved.
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

use lib 'lib';
use lib "$ENV{RQG_HOME}/lib";
use lib 'randgen/lib';

use strict;
use Carp;
use Cwd;
use DBI;
use File::Find;
use GenTest::Random;
use POSIX;
use Sys::Hostname;

my ($basedir, $vardir, $tree, $test) = @ARGV;

#
# For further details about tests and recommended RQG options, see
# http://forge.mysql.com/wiki/RandomQueryGeneratorTests
#

print("==================== Starting $0 ====================\n");
# Print MTR-style output saying which test suite/mode this is for PB2 reporting.
# So far we only support running one test at a time.
print("##############################################################################\n");
print("# $test\n");
print("##############################################################################\n");

# Autoflush output buffers (needed when using POSIX::_exit())
$| = 1;

#
# Check OS. Windows and Unix/Linux are too different.
#
my $windowsOS;
if (
	($^O eq 'MSWin32') ||
	($^O eq 'MSWin64')
) {
	$windowsOS = 'true';
}

#
# Prepare ENV variables and other settings.
#

# Local "installation" of MySQL 5.0. Default is for Unix hosts. See below for Windows.
my $basedirRelease50 = '/export/home/mysql-releases/mysql-5.0';

# Location of grammars and other test configuration files.
# Will use env variable RQG_CONF if set.
# Default is currently "conf" while using legacy setup.
# If not absolute path, it is relative to cwd at run time, which is the randgen directory.
my $conf = $ENV{RQG_CONF};
$conf = 'conf' if not defined $conf;

if ($windowsOS) {
	# For tail and for cdb
	$ENV{PATH} = 'G:\pb2\scripts\randgen\bin;G:\pb2\scripts\bin;C:\Program Files\Debugging Tools for Windows (x86);'.$ENV{PATH};
	$ENV{_NT_SYMBOL_PATH} = 'srv*c:\\cdb_symbols*http://msdl.microsoft.com/download/symbols;cache*c:\\cdb_symbols';

	# For vlad
	#ENV{MYSQL_FULL_MINIDUMP} = 1;

	#system("date /T");
	#system("time /T");

	# Path to MySQL releases used for comparison runs.
	$basedirRelease50 = 'G:\mysql-releases\mysql-5.0.87-win32'; # loki06
} elsif ($^O eq 'solaris') {
	# For libmysqlclient
	$ENV{LD_LIBRARY_PATH}=$ENV{LD_LIBRARY_PATH}.':/export/home/pb2/scripts/lib/';

	# For DBI and DBD::mysql
	$ENV{PERL5LIB}=$ENV{PERL5LIB}.':/export/home/pb2/scripts/DBI-1.607/:/export/home/pb2/scripts/DBI-1.607/lib:/export/home/pb2/scripts/DBI-1.607/blib/arch/:/export/home/pb2/scripts/DBD-mysql-4.008/lib/:/export/home/pb2/scripts/DBD-mysql-4.008/blib/arch/';
	
	# For c++filt
	$ENV{PATH} = $ENV{PATH}.':/opt/studio12/SUNWspro/bin';

	#system("uname -a");
	#system("date");

}

################################################################################
##
## subroutines
##
################################################################################

#
# Skips the test, displays reason (argument to the routine) quasi-MTR-style and
# exits with exit code 0.
#
# Example usage:
#   # This feature is not yet supported on Windows, so skip this test
#   skip_test("This feature/test does not support the Windows platform at this time");
#
# will appear in output as:
#   rpl_semisync                             [ skipped ] This feature/test does not support the Windows platform at this time
#
sub skip_test {
	my $reason = @_[0];
	my $message = "$test";
	# Using MTR-style output for the readers' convenience.
	# (at least 41 chars before "[ skipped ]")
	while (length $message < 40)
	{
		$message = $message.' ';
	}
	$message = $message." [ skipped ] ".$reason;
	print "$message\n";
	print localtime()." [$$] $0 will exit with exit status 0.\n";
	POSIX::_exit (0);
}

#
# Returns a random number between 1 and 499.
#
sub pick_random_port_range_id {
	my $prng = GenTest::Random->new( seed => time );
	return $prng->uint16(1,499);
}

#
# Searches recursively for a given file name under the given directory.
# Default top search directory is $basedir.
#
# Arg1 (mandatory): file name (excluding path)
# Arg2 (optional) : directory where search will start
#
# Returns full path to the directory where the file resides, if found.
# If more than one matching file is found, the directory of the first one found
# in a depth-first search will be returned.
# Returns undef if none is found.
#
sub findDirectory {
	my ($plugin_name, $dir) = @_;
	if (not defined $plugin_name) {
		carp("File name required as argument to subroutine findDirectory()");
	}
	if (not defined $dir) {
		$dir = $basedir;
	}
	my $fullPath;	# the result
	find(sub {
			# This subroutine is called for each file and dir it finds.
			# According to docs it does depth-first search.
			if ($_ eq $plugin_name) {
				$fullPath = $File::Find::dir if not defined $fullPath;
			}
			# any return value is ignored
		}, $dir);
	return $fullPath;
}

#
# Get the bzr branch ID from the pushbuild2 database (internal), based on the
# branch name ($tree variable).
#
# If the branch name (tree) is not found in the database, or we are unable to
# connect to the database, undef is returned.
#
sub get_pb2_branch_id {

	# First, check if the environment variable BRANCH_ID is set.
	if (defined $ENV{BRANCH_ID}) {
		return $ENV{BRANCH_ID};
	} else {
		# Disable db lookup for the time being due to issues on sparc32.
		# Remove this "else" block to enable
		return;
	}
	# Lookup by branch name. Get branch name from tree, which could be url.
	my $branch_name = $tree;
	if ($tree =~ m{/}) {
		# Found '/', assuming tree is URL.
		# Find last substring that is between a '/' and either end-of-string or a '/' followed by end of string.
		$tree =~ m{.*/([^/]+)($|/$)};
		$branch_name=$1;
	}

	my $dsn_pb2 = 'dbi:mysql:host=trollheim.norway.sun.com:port=3306:user=readonly:database=pushbuild2';
	my $SQL_getBranchId = "SELECT branch_id FROM branches WHERE branch_name = '$branch_name'";

	print("Using branch name $branch_name\n");
	print("Trying to connect to pushbuild2 database...\n");

	my $dbh = DBI->connect($dsn_pb2, undef, undef, {
		mysql_connect_timeout => 5,
		PrintError => 0,
		RaiseError => 0,
		AutoCommit => 0,
	} );

	if (not defined $dbh) {
		print("connect() to pushbuild2 database failed: ".$DBI::errstr."\n");
		return;
	}

	my $id = $dbh->selectrow_array($SQL_getBranchId);
	$dbh->disconnect;
	return $id;
}

#### end subroutines ###########################################################

chdir('randgen');

print("===== Information on the host system: =====\n");
print(" - Local time  : ".localtime()."\n");
print(" - Hostname    : ".hostname()."\n");
print(" - PID         : $$\n");
print(" - Working dir : ".cwd()."\n");
print(" - PATH        : ".$ENV{'PATH'}."\n");
print(" - Script arguments:\n");
print("       basedir = $basedir\n");
print("       vardir  = $vardir\n");
print("       tree    = $tree\n");
print("       test    = $test\n");
print("\n");
print("===== Information on Random Query Generator version (bzr): =====\n");
system("bzr info");
system("bzr version-info");
print("\n");

# Test name:
#   In PB2, tests run via this script are prefixed with "rqg_" so that it is
#   easy to distinguish these tests from other "external" tests.
#   For a while we will support test names both with and without the prefix.
#   For this reason we strip off the "rqg_" prefix before continuing.
#   This also means that you cannot try to match against "rqg_" prefix in test
#   "definitions" (if statements) below.
my $test_name = $test;
my $test_suite_name = 'serverqa'; # used for xref reporting
$test =~ s/^rqg_//;	# test_name without prefix

# Server port numbers:
#
# If several instances of this script may run at the same time on the same
# host, port number conflicts may occur.
#
# If needed, use use a port range ID (integer) that is unique for this host at
# this time.
# This ID is used by the RQG framework to designate a port range to use for the
# test run. Passed to RQG using the MTR_BUILD_THREAD environment variable
# (this naming is a legacy from MTR, which is used by RQG to start the MySQL
# server).
#
# Solution: Use unique port range id per branch. Use "branch_id" as recorded
#           in PB2 database (guaranteed unique per branch).
# Potential issue 1: Unable to connect to pb2 database.
# Solution 1: Pick a random ID between 1 and some sensible number (e.g. 500).
# Potential issue 2: Clashing resources when running multiple pushes in same branch?
# Potential solution 2: Keep track of used ids in local file(s). Pick unused id.
#                       (not implemented yet)
#
# Currently (December 2009) PB2 RQG host should be running only one test at a
# time, so this should not be an issue, hence no need to set MTR_BUILD_THREAD.

#print("===== Determining port base id: =====\n");
my $port_range_id; # Corresponding to MTR_BUILD_THREAD in the MySQL MTR world.
# First, see if user has supplied us with a value for MTR_BUILD_THREAD:
$port_range_id = $ENV{MTR_BUILD_THREAD};
if (defined $port_range_id) {
	print("Environment variable MTR_BUILD_THREAD was already set.\n");
}
#else {
#	# try to obtain branch id, somehow
#	$port_range_id = get_pb2_branch_id();
#	if (not defined $port_range_id) {
#		print("Unable to get branch id. Picking a 'random' port base id...\n");
#		$port_range_id = pick_random_port_range_id();
#	} else {
#		print("Using pb2 branch ID as port base ID.\n");
#	}
#}

print("Configuring test...\n");

my $cwd = cwd();

my $command;
my $engine;
my $rpl_mode;

if (($engine) = $test =~ m{(maria|falcon|innodb|myisam|pbxt)}io) {
	print "Detected that this test is about the $engine engine.\n";
}

if (($rpl_mode) = $test =~ m{(rbr|sbr|mbr|statement|mixed|row)}io) {
	print "Detected that this test is about replication mode $rpl_mode.\n";
	$rpl_mode = 'mixed' if $rpl_mode eq 'mbr';
	$rpl_mode = 'statement' if $rpl_mode eq 'sbr';
	$rpl_mode = 'row' if $rpl_mode eq 'rbr';
}

#
# Start defining tests. Test name can be whatever matches the regex in the if().
# TODO: Define less ambiguous test names to avoid accidental misconfiguration.
#
# Starting out with "legacy" Falcon tests.
#
if ($test =~ m{falcon_.*transactions}io ) {
	$command = '
		--grammar='.$conf.'/transactions.yy
		--gendata='.$conf.'/transactions.zz
		--mysqld=--falcon-consistent-read=1
		--mysqld=--transaction-isolation=REPEATABLE-READ
		--validator=DatabaseConsistency
		--mem
	';
} elsif ($test =~ m{falcon_.*durability}io ) {
	$command = '
		--grammar='.$conf.'/transaction_durability.yy
		--vardir1='.$vardir.'/vardir-'.$engine.'
		--vardir2='.$vardir.'/vardir-innodb
		--mysqld=--default-storage-engine='.$engine.'
		--mysqld=--falcon-checkpoint-schedule=\'1 1 1 1 1\'
		--mysqld2=--default-storage-engine=Innodb
		--validator=ResultsetComparator
	';
} elsif ($test =~ m{falcon_repeatable_read}io ) {
	$command = '
		--grammar='.$conf.'/repeatable_read.yy
		--gendata='.$conf.'/transactions.zz
		--mysqld=--falcon-consistent-read=1
		--mysqld=--transaction-isolation=REPEATABLE-READ
		--validator=RepeatableRead
		--mysqld=--falcon-consistent-read=1
		--mem
	';
} elsif ($test =~ m{falcon_chill_thaw_compare}io) {
	$command = '
	        --grammar='.$conf.'/falcon_chill_thaw.yy
		--gendata='.$conf.'/falcon_chill_thaw.zz
	        --mysqld=--falcon-record-chill-threshold=1K
	        --mysqld=--falcon-index-chill-threshold=1K 
		--threads=1
		--vardir1='.$vardir.'/chillthaw-vardir
		--vardir2='.$vardir.'/default-vardir
		--reporters=Deadlock,ErrorLog,Backtrace
	';
} elsif ($test =~ m{falcon_chill_thaw}io) {
	$command = '
	        --grammar='.$conf.'/falcon_chill_thaw.yy
	        --mysqld=--falcon-index-chill-threshold=4K 
	        --mysqld=--falcon-record-chill-threshold=4K
	';
} elsif ($test =~ m{falcon_online_alter}io) {
	$command = '
	        --grammar='.$conf.'/falcon_online_alter.yy
	';
} elsif ($test =~ m{falcon_ddl}io) {
	$command = '
	        --grammar='.$conf.'/falcon_ddl.yy
	';
} elsif ($test =~ m{falcon_limit_compare_self}io ) {
	$command = '
		--grammar='.$conf.'/falcon_nolimit.yy
		--threads=1
		--validator=Limit
	';
} elsif ($test =~ m{falcon_limit_compare_innodb}io ) {
	$command = '
		--grammar='.$conf.'/limit_compare.yy
		--vardir1='.$vardir.'/vardir-falcon
		--vardir2='.$vardir.'/vardir-innodb
		--mysqld=--default-storage-engine=Falcon
		--mysqld2=--default-storage-engine=Innodb
		--threads=1
		--reporters=
	';
} elsif ($test =~ m{falcon_limit}io ) {
	$command = '
	        --grammar='.$conf.'/falcon_limit.yy
		--mysqld=--loose-maria-pagecache-buffer-size=64M
	';
} elsif ($test =~ m{falcon_recovery}io ) {
	$command = '
	        --grammar='.$conf.'/falcon_recovery.yy
		--gendata='.$conf.'/falcon_recovery.zz
		--mysqld=--falcon-checkpoint-schedule="1 1 1 1 1"
	';
} elsif ($test =~ m{falcon_pagesize_32K}io ) {
	$command = '
		--grammar='.$conf.'/falcon_pagesize.yy
		--mysqld=--falcon-page-size=32K
		--gendata='.$conf.'/falcon_pagesize32K.zz
	';
} elsif ($test =~ m{falcon_pagesize_2K}io) {
	$command = '
		--grammar='.$conf.'/falcon_pagesize.yy
		--mysqld=--falcon-page-size=2K
		--gendata='.$conf.'/falcon_pagesize2K.zz
	';
} elsif ($test =~ m{falcon_select_autocommit}io) {
	$command = '
		--grammar='.$conf.'/falcon_select_autocommit.yy
		--queries=10000000
	';
} elsif ($test =~ m{falcon_backlog}io ) {
	$command = '
		--grammar='.$conf.'/falcon_backlog.yy
		--gendata='.$conf.'/falcon_backlog.zz
		--mysqld=--transaction-isolation=REPEATABLE-READ
		--mysqld=--falcon-record-memory-max=10M
		--mysqld=--falcon-record-chill-threshold=1K
		--mysqld=--falcon-page-cache-size=128M
	';
} elsif ($test =~ m{falcon_compare_innodb}io ) {
        # Datatypes YEAR and TIME disabled in grammars due to Bug#45499 (InnoDB). 
        # Revert to falcon_data_types.{yy|zz} when that bug is resolved in relevant branches.
	$command = '
		--grammar='.$conf.'/falcon_data_types_no_year_time.yy
		--gendata='.$conf.'/falcon_data_types_no_year_time.zz
		--vardir1='.$vardir.'/vardir-falcon
		--vardir2='.$vardir.'/vardir-innodb
		--mysqld=--default-storage-engine=Falcon
		--mysqld2=--default-storage-engine=Innodb
		--threads=1
		--reporters=
	';
} elsif ($test =~ m{falcon_compare_self}io ) {
	$command = '
		--grammar='.$conf.'/falcon_data_types.yy
		--gendata='.$conf.'/falcon_data_types.zz
		--vardir1='.$vardir.'/'.$engine.'-vardir1
		--vardir2='.$vardir.'/'.$engine.'-vardir2
		--threads=1
		--reporters=
	';
#
# END OF FALCON-ONLY TESTS
#
} elsif ($test =~ m{innodb_repeatable_read}io ) {
	# Transactional test. See also falcon_repeatable_read.
	$command = '
		--grammar='.$conf.'/repeatable_read.yy
		--gendata='.$conf.'/transactions.zz
		--mysqld=--transaction-isolation=REPEATABLE-READ
		--validator=RepeatableRead
	';
} elsif ($test =~ m{(falcon|myisam)_blob_recovery}io ) {
	$command = '
		--grammar='.$conf.'/falcon_blobs.yy
		--gendata='.$conf.'/falcon_blobs.zz
		--duration=130
		--threads=1
		--reporters=Deadlock,ErrorLog,Backtrace,Recovery,Shutdown
	';
	if ($test =~ m{falcon}io) {
		# this option works with Falcon-enabled builds only
		$command = $command.'
			--mysqld=--falcon-page-cache-size=128M
		';
	}
} elsif ($test =~ m{(falcon|innodb|myisam)_many_indexes}io ) {
	$command = '
		--grammar='.$conf.'/many_indexes.yy
		--gendata='.$conf.'/many_indexes.zz
	';
} elsif ($test =~ m{(falcon|innodb|myisam)_tiny_inserts}io) {
	$command = '
		--gendata='.$conf.'/tiny_inserts.zz
		--grammar='.$conf.'/tiny_inserts.yy
		--queries=10000000
	';
} elsif ($test =~ m{innodb_transactions}io) {
	$command = '
		--grammar='.$conf.'/transactions.yy
		--gendata='.$conf.'/transactions.zz
		--mysqld=--transaction-isolation=REPEATABLE-READ
		--validator=DatabaseConsistency
	';
#
# END OF STORAGE ENGINE TESTS
#
# Keep the following tests in alphabetical order (based on letters in regex)
# for easy lookup.
#
} elsif ($test =~ m{^backup_.*?_simple}io) {
	$command = '
		--grammar='.$conf.'/backup_simple.yy
		--reporters=Deadlock,ErrorLog,Backtrace
		--mysqld=--mysql-backup
	';
} elsif ($test =~ m{^backup_.*?_consistency}io) {
	$command = '
		--gendata='.$conf.'/invariant.zz
		--grammar='.$conf.'/invariant.yy
		--validator=Invariant
		--reporters=Deadlock,ErrorLog,Backtrace,BackupAndRestoreInvariant,Shutdown
		--mysqld=--mysql-backup
		--duration=600
		--threads=25
	';
} elsif ($test =~ m{dml_alter}io ) {
	$command = '
		--gendata='.$conf.'/maria.zz
		--grammar='.$conf.'/maria_dml_alter.yy
	';
} elsif ($test =~ m{^info_schema}io ) {
	$command = '
		--grammar='.$conf.'/information_schema.yy
		--threads=10
		--duration=300
		--mysqld=--log-output=file
	';
} elsif ($test =~ m{^mdl_stability}io ) {
	$command = '
		--grammar='.$conf.'/metadata_stability.yy
		--gendata='.$conf.'/metadata_stability.zz
		--validator=SelectStability,QueryProperties
		--engine=Innodb
		--mysqld=--innodb
		--mysqld=--default-storage-engine=Innodb
		--mysqld=--transaction-isolation=SERIALIZABLE
		--mysqld=--innodb-flush-log-at-trx-commit=2
		--mysqld=--table-lock-wait-timeout=1
		--mysqld=--innodb-lock-wait-timeout=1
		--mysqld=--log-output=file
		--queries=1M
		--duration=600
	';
} elsif ($test =~ m{^mdl_stress}io ) {
	# Seems like --gendata=conf/WL5004_data.zz unexplicably causes more test
	# failures, so let's leave this out of PB2 for the time being (pstoev).
	#
	# InnoDB should be loaded but the test is not per se for InnoDB, hence
	# no "innodb" in test name.
	$command = '
		--grammar=conf/WL5004_sql.yy
		--threads=10
		--queries=1M
		--duration=1800
		--mysqld=--innodb
	';
} elsif ($test =~ m{^partition_ddl}io ) {
	$command = '
		--grammar='.$conf.'/partitions-ddl.yy
		--mysqld=--innodb
		--threads=1
		--queries=100K
	';
} elsif ($test =~ m{partn_pruning(|.valgrind)$}io ) {
	# reduced duration to half since gendata phase takes longer in this case
	$command = '
		--gendata='.$conf.'/partition_pruning.zz
		--grammar='.$conf.'/partition_pruning.yy
		--mysqld=--innodb
		--threads=1
		--queries=100000
		--duration=300
	';
} elsif ($test =~ m{^partn_pruning_compare_50}io) {
	$command = '
	--gendata='.$conf.'/partition_pruning.zz
	--grammar='.$conf.'/partition_pruning.yy
	--basedir1='.$basedir.'
	--basedir2='.$basedirRelease50.'
	--vardir1='.$vardir.'/vardir-bzr
	--vardir2='.$vardir.'/vardir-5.0
	--mysqld=--innodb
	--validators=ResultsetComparator
	--reporters=Deadlock,ErrorLog,Backtrace
	--threads=1
	--queries=10000
	--duration=300
	';
} elsif ($test =~ m{^rpl_.*?_simple}io) {
	# Not used; rpl testing needs adjustments (some of the failures this
	# produces are known replication issues documented in the manual).
	$command = '
		--gendata='.$conf.'/replication_single_engine.zz
		--grammar='.$conf.'/replication_simple.yy
		--mysqld=--log-output=table,file
	';
} elsif ($test =~ m{^rpl_.*?_complex}io) {
	# Not used; rpl testing needs adjustments (some of the failures this
	# produces are known replication issues documented in the manual).
	$command = '
		--gendata='.$conf.'/replication_single_engine_pk.zz
		--grammar='.$conf.'/replication.yy
		--mysqld=--log-output=table,file
		--mysqld=--innodb
	';
} elsif ($test =~ m{^rpl_semisync}io) {
	# --rpl_mode=default is used because the .YY file changes the binary log format dynamically.
	# --threads=1 is used to avoid any replication failures due to concurrent DDL.
	# --validator= line will remove the default replication Validator, which would otherwise
	#   report test failure when the slave I/O thread is stopped, which is OK in the context
	#   of this particular test.

	# Plugin file names and location vary between platforms.
	# See http://bugs.mysql.com/bug.php?id=49170 for details.
	# We search for the respective file names under basedir (recursively).
	# The first matching file that is found is used.
	# We assume that both master and slave plugins are in the same dir.
	# Unix file name extenstions other than .so may exist, but support for this
	# is not yet implemented here.
	my $plugin_dir;
	my $plugins;
	if ($windowsOS) {
		my $master_plugin_name = "semisync_master.dll";
		$plugin_dir=findDirectory($master_plugin_name);
		if (not defined $plugin_dir) {
			carp "Unable to find semisync plugin $master_plugin_name!";
		}
		$plugins = 'rpl_semi_sync_master='.$master_plugin_name.';rpl_semi_sync_slave=semisync_slave.dll';
	} else {
		# tested on Linux and Solaris
		my $prefix;	# for Bug#48351
		my $master_plugin_name = "semisync_master.so";
		$plugin_dir=findDirectory($master_plugin_name);
		if (not defined $plugin_dir) {
			# Until fix for Bug#48351 is widespread it may happen
			# that the Unix plugin names are prefixed with "lib".
			# Remove this when no longer needed.
			$prefix = 'lib';
			$plugin_dir=findDirectory($prefix.$master_plugin_name);
			if (not defined $plugin_dir) {
				carp "Unable to find semisync plugin! ($master_plugin_name or $prefix$master_plugin_name)";
			}
		}
		$plugins = 'rpl_semi_sync_master='.$prefix.$master_plugin_name.':rpl_semi_sync_slave='.$prefix.'semisync_slave.so';
	}
	$command = '
		--gendata='.$conf.'/replication_single_engine.zz
		--engine=InnoDB
		--grammar='.$conf.'/replication_simple.yy
		--rpl_mode=default
		--mysqld=--plugin-dir='.$plugin_dir.'
		--mysqld=--plugin-load='.$plugins.'
		--mysqld=--rpl_semi_sync_master_enabled=1
		--mysqld=--rpl_semi_sync_slave_enabled=1
		--mysqld=--innodb
		--reporters=ReplicationSemiSync,Deadlock,Backtrace,ErrorLog
		--validators=None
		--threads=1
		--duration=300
		--queries=1M
	';
} elsif ($test =~ m{signal_resignal}io ) {
	$command = '
		--threads=10
		--queries=1M
		--duration=300
		--grammar='.$conf.'/signal_resignal.yy
		--mysqld=--max-sp-recursion-depth=10
	';
} elsif ($test =~ m{(innodb|maria|myisam)_stress}io ) {
	$command = '
		--grammar='.$conf.'/maria_stress.yy
	';
} else {
	print("[ERROR]: Test configuration for test name '$test' is not ".
		"defined in this script.\n");
	my $exitCode = 1;
	print("Will exit $0 with exit code $exitCode.\n");
	POSIX::_exit ($exitCode);
}

#
# Specify some "default" Reporters if none have been specified already.
# The RQG itself also specifies some default values for some options if not set.
#
if ($command =~ m{--reporters}io) {
	# Reporters have already been specified	
} elsif ($test =~ m{rpl}io ) {
	# Don't include Recovery for replication tests, because
	$command = $command.' --reporters=Deadlock,ErrorLog,Backtrace';
} elsif ($test =~ m{falcon}io ) {
	# Include the Recovery reporter for Falcon tests in order to test
	# recovery by default after each such test.
	$command = $command.' --reporters=Deadlock,ErrorLog,Backtrace,Recovery,Shutdown';
	# Falcon-only options (avoid "unknown variable" warnings in non-Falcon builds)
	$command = $command.' --mysqld=--loose-falcon-lock-wait-timeout=5 --mysqld=--loose-falcon-debug-mask=2';
} else {
	# Default reporters for tests whose name does not contain "rpl" or "falcon"
	$command = $command.' --reporters=Deadlock,ErrorLog,Backtrace,Shutdown';
}

#
# Other defaults...
#

if ($command !~ m{--duration}io ) {
	# Set default duration for tests where duration is not specified.
	# In PB2 we cannot run tests for too long since there are many branches
	# and many pushes (or other test triggers).
	# Setting it to 10 minutes for now.
	$command = $command.' --duration=600';
}

if ($command !~ m{--basedir}io ) {
	$command = $command." --basedir=\"$basedir\"";
}

if ($command !~ m{--vardir}io && $command !~ m{--mem}io ) {
	$command = $command." --vardir=\"$vardir\"";
}

if ($command !~ m{--log-output}io) {
	$command = $command.' --mysqld=--log-output=file';
}

if ($command !~ m{--queries}io) {
	$command = $command.' --queries=100000';
}

if (($command !~ m{--(engine|default-storage-engine)}io) && (defined $engine)) {
	$command = $command." --engine=$engine";
}

# if test name contains "innodb", add the --mysqld=--innodb and --engine=innodb 
# options if they are not there already.
if ( ($test =~ m{innodb}io) ){
	if ($command !~ m{mysqld=--innodb}io){
		$command = $command.' --mysqld=--innodb';
	}
	if ($command !~ m{engine=innodb}io){
		$command = $command.' --engine=innodb';
	}
}

if (($command !~ m{--rpl_mode}io)  && ($rpl_mode ne '')) {
	$command = $command." --rpl_mode=$rpl_mode";
}

# if test name contains (usually ends with) "valgrind", add the valgrind option to runall.pl
if ($test =~ m{valgrind}io){
	print("Detected that this test should enable valgrind instrumentation.\n");
	if (system("valgrind --version")) {
		print("  *** valgrind executable not found! Not setting --valgrind flag.\n");
	} else {
		$command = $command.' --valgrind';
	}
}
	
$command = "perl runall.pl --mysqld=--loose-innodb-lock-wait-timeout=5 --mysqld=--table-lock-wait-timeout=5 --mysqld=--loose-skip-safemalloc ".$command;

# Add env variable to specify unique port range to use to avoid conflicts.
# Trying not to do this unless actually needed.
if (defined $port_range_id) {
	print("MTR_BUILD_THREAD=$port_range_id\n");
	if ($windowsOS) {
		$command = "set MTR_BUILD_THREAD=$port_range_id && ".$command;
	} else {
		$command = "MTR_BUILD_THREAD=$port_range_id ".$command;
	}
}

$command =~ s{[\r\n\t]}{ }sgio;
print("Running runall.pl...\n");
my $command_result = system($command);
# shift result code to the right to obtain the code returned from the called script
my $command_result_shifted = ($command_result >> 8);

# Report test result in an MTR fashion so that PB2 will see it and add to
# xref database etc.
# Format: TESTSUITE.TESTCASE 'TESTMODE' [ RESULT ]
# Example: ndb.ndb_dd_alter 'InnoDB plugin'     [ fail ]
# Not using TESTMODE for now.

my $full_test_name = $test_suite_name.'.'.$test_name;
# keep test statuses more or less vertically aligned
while (length $full_test_name < 40)
{
	$full_test_name = $full_test_name.' ';
}

if ($command_result_shifted > 0) {
	# test failed
	# Trying out marking a test as "experimental" by reporting exp-fail:
	# Mark all failures in next-mr-johnemb as experimental (temporary).
	if ($ENV{BRANCH_NAME} =~ m{mysql-next-mr-johnemb}) {
		print($full_test_name." [ exp-fail ]\n");
	} else {
		print($full_test_name." [ fail ]\n");
	}
	print('runall.pl failed with exit code '.$command_result_shifted."\n");
	print("Look above this message in the test log for failure details.\n");
} else {
	print($full_test_name." [ pass ]\n");
}



# Kill remaining mysqld processes.
# Assuming only one test run going on at the same time, and that all mysqld
# processes are ours.
print("Checking for remaining mysqld processes...\n");
if ($windowsOS) {
	# assumes MS Sysinternals PsTools is installed in C:\bin
	# If you need to run pslist or pskill as non-Admin user, some adjustments
	# may be needed. See:
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

print(" [$$] $0 will exit with exit status ".$command_result_shifted."\n");
POSIX::_exit ($command_result_shifted);
