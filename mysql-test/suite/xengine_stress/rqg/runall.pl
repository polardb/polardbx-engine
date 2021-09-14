#!/usr/bin/perl

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

#
# This script executes the following sequence
#
# $ mysql-test-run.pl --start-and-exit with replication
# $ gentest.pl --gendata
# $ diff master slave
#
#

use lib 'lib';
use lib "$ENV{RQG_HOME}/lib";
use strict;
use GenTest;

$| = 1;
if (windows()) {
	$SIG{CHLD} = "IGNORE";
}

if (defined $ENV{RQG_HOME}) {
        if (windows()) {
                $ENV{RQG_HOME} = $ENV{RQG_HOME}.'\\';
        } else {
                $ENV{RQG_HOME} = $ENV{RQG_HOME}.'/';
        }
}

use Getopt::Long;
use GenTest::Constants;
use DBI;
use Cwd;

my $database = 'test';
my @master_dsns;

my ($gendata, @basedirs, @mysqld_options, @vardirs, $rpl_mode,
    $engine, $help, $debug, $validators, $reporters, $grammar_file,
    $redefine_file, $seed, $mask, $mask_level, $mem, $rows,
    $varchar_len, $xml_output, $valgrind, $views, $start_dirty,
    $filter, $build_thread);

my $threads = my $default_threads = 10;
my $queries = my $default_queries = 1000;
my $duration = my $default_duration = 3600;

my @ARGV_saved = @ARGV;

my $opt_result = GetOptions(
	'mysqld=s@' => \$mysqld_options[0],
	'mysqld1=s@' => \$mysqld_options[0],
	'mysqld2=s@' => \$mysqld_options[1],
	'basedir=s' => \$basedirs[0],
	'basedir1=s' => \$basedirs[0],
	'basedir2=s' => \$basedirs[1],
	'vardir=s' => \$vardirs[0],
	'vardir1=s' => \$vardirs[0],
	'vardir2=s' => \$vardirs[1],
	'rpl_mode=s' => \$rpl_mode,
	'engine=s' => \$engine,
	'grammar=s' => \$grammar_file,
	'redefine=s' => \$redefine_file,
	'threads=i' => \$threads,
	'queries=s' => \$queries,
	'duration=i' => \$duration,
	'help' => \$help,
	'debug' => \$debug,
	'validators:s' => \$validators,
	'reporters:s' => \$reporters,
	'gendata:s' => \$gendata,
	'seed=s' => \$seed,
	'mask=i' => \$mask,
        'mask-level=i' => \$mask_level,
	'mem' => \$mem,
	'rows=i' => \$rows,
	'varchar-length=i' => \$varchar_len,
	'xml-output=s'	=> \$xml_output,
	'valgrind'	=> \$valgrind,
	'views'		=> \$views,
	'start-dirty'	=> \$start_dirty,
	'filter=s'	=> \$filter,
    'mtr-build-thread=i' => \$build_thread
);

if (!$opt_result || $help || $basedirs[0] eq '' || not defined $grammar_file) {
	help();
	exit($help ? 0 : 1);
}

say("Copyright (c) 2008-2009 Sun Microsystems, Inc. All rights reserved. Use is subject to license terms.");
say("Please see http://forge.mysql.com/wiki/Category:RandomQueryGenerator for more information on this test framework.");
say("Starting \n# $0 \\ \n# ".join(" \\ \n# ", @ARGV_saved));

#
# Calculate master and slave ports based on MTR_BUILD_THREAD (MTR
# Version 1 behaviour)
#

if (not defined $build_thread) {
    if (defined $ENV{MTR_BUILD_THREAD}) {
        $build_thread = $ENV{MTR_BUILD_THREAD}
    } else {
        $build_thread = DEFAULT_MTR_BUILD_THREAD;
    }
}

if ( $build_thread eq 'auto' ) {
    say ("Please set the environment variable MTR_BUILD_THREAD to a value <> 'auto' (recommended) or unset it (will take the value ".DEFAULT_MTR_BUILD_THREAD.") ");
    exit (STATUS_ENVIRONMENT_FAILURE);
}

my $master_port = 10000 + 10 * $build_thread;
my $slave_port = 10000 + 10 * $build_thread + 2;
my @master_ports = ($master_port,$slave_port);

say("master_port : $master_port slave_port : $slave_port master_ports : @master_ports MTR_BUILD_THREAD : $build_thread ");

$ENV{MTR_BUILD_THREAD} = $build_thread;

#
# If the user has provided two vardirs and one basedir, start second
# server using the same basedir
#


if (
	($vardirs[1] ne '') && 
	($basedirs[1] eq '')
) {
	$basedirs[1] = $basedirs[0];	
}


if (
	($mysqld_options[1] ne '') && 
	($basedirs[1] eq '')
) {
	$basedirs[1] = $basedirs[0];	
}

#
# If the user has provided identical basedirs and vardirs, warn of a potential overlap.
#

if (
	($basedirs[0] eq $basedirs[1]) &&
	($vardirs[0] eq $vardirs[1]) &&
	($rpl_mode eq '')
) {
	die("Please specify either different --basedir[12] or different --vardir[12] in order to start two MySQL servers");
}

my $client_basedir;
	
foreach my $path ("$basedirs[0]/client/RelWithDebInfo", "$basedirs[0]/client", "$basedirs[0]/bin") {
	if (-e $path) {
		$client_basedir = $path;
		last;
	}
}

my $cwd = cwd();

#
# Start servers. Use rpl_alter if replication is needed.
#
	
foreach my $server_id (0..1) {
	next if $basedirs[$server_id] eq '';

	if (
		($server_id == 0) ||
		($rpl_mode eq '') 
	) {
		$master_dsns[$server_id] = "dbi:mysql:host=127.0.0.1:port=".$master_ports[$server_id].":user=root:database=".$database;
	}

	my @mtr_options;
	push @mtr_options, lc("--mysqld=--$engine") if defined $engine && lc($engine) ne lc('myisam');

	push @mtr_options, "--mem" if defined $mem;
	push @mtr_options, "--valgrind" if defined $valgrind;

	push @mtr_options, "--skip-ndb";
	push @mtr_options, "--mysqld=--core-file";
	push @mtr_options, "--mysqld=--loose-new";
#	push @mtr_options, "--mysqld=--default-storage-engine=$engine" if defined $engine;
	push @mtr_options, "--mysqld=--sql-mode=no_engine_substitution" if join(' ', @ARGV_saved) !~ m{sql-mode}io;
	push @mtr_options, "--mysqld=--relay-log=slave-relay-bin";
	push @mtr_options, "--mysqld=--loose-innodb";
	push @mtr_options, "--mysqld=--loose-falcon-debug-mask=2";
	push @mtr_options, "--mysqld=--secure-file-priv=";		# Disable secure-file-priv that mtr enables.
	push @mtr_options, "--mysqld=--max-allowed-packet=16Mb";	# Allow loading bigger blobs
	push @mtr_options, "--mysqld=--loose-innodb-status-file=1";
	push @mtr_options, "--mysqld=--master-retry-count=65535";

	push @mtr_options, "--start-dirty" if defined $start_dirty;
#	push @mtr_options, "--gcov";

	if (($rpl_mode ne '') && ($server_id != 0)) {
		# If we are running in replication, and we start the slave separately (because it is a different binary)
		# add a few options that allow the slave and the master to be distinguished and SHOW SLAVE HOSTS to work
		push @mtr_options, "--mysqld=--server-id=".($server_id + 1);
		push @mtr_options, "--mysqld=--report-host=127.0.0.1";
		push @mtr_options, "--mysqld=--report-port=".$master_ports[$server_id];
	}

	my $mtr_path = $basedirs[$server_id].'/mysql-test/';
	chdir($mtr_path) or die "unable to chdir() to $mtr_path: $!";
	
	push @mtr_options, "--vardir=$vardirs[$server_id]" if defined $vardirs[$server_id];
	push @mtr_options, "--master_port=".$master_ports[$server_id];

	if (defined $mysqld_options[$server_id]) {
		foreach my $mysqld_option (@{$mysqld_options[$server_id]}) {
			push @mtr_options, '--mysqld="'.$mysqld_option.'"';
		}
	}

	if (
		($rpl_mode ne '') &&
		($server_id == 0) &&
		(not defined $vardirs[1]) &&
		(not defined $mysqld_options[1])
	) {
		push @mtr_options, 'rpl_alter';
		push @mtr_options, "--slave_port=".$slave_port;
	} elsif ($basedirs[$server_id] =~ m{(^|-)5\.0}sgio) {
		say("Basedir implies server version 5.0. Will not use --start-and-exit 1st");
		# Do nothing, test name "1st" does not exist in 5.0
	} else {
		push @mtr_options, '1st';
	}

	$ENV{MTR_VERSION} = 1;
#	my $out_file = "/tmp/mtr-".$$."-".$server_id.".out";
	my $mtr_command = "perl mysql-test-run.pl --start-and-exit ".join(' ', @mtr_options)." 2>&1";
	say("Running $mtr_command .");

	my $vardir = $vardirs[$server_id] || $basedirs[$server_id].'/mysql-test/var';

	open (MTR_COMMAND, '>'.$mtr_path.'/mtr_command') or say("Unable to open mtr_command: $!");
	print MTR_COMMAND $mtr_command;
	close MTR_COMMAND;

	my $mtr_status = system($mtr_command);

	if ($mtr_status != 0) {
#		system("cat $out_file");
		system("cat \"$vardir/log/master.err\"");
		exit_test($mtr_status >> 8);
	}
#	unlink($out_file);
	
	if ((defined $master_dsns[$server_id]) && (defined $engine)) {
		my $dbh = DBI->connect($master_dsns[$server_id], undef, undef, { RaiseError => 1 } );
		$dbh->do("SET GLOBAL storage_engine = '$engine'");
	}
}

chdir($cwd);

my $master_dbh = DBI->connect($master_dsns[0], undef, undef, { RaiseError => 1 } );

if ($rpl_mode) {
	my $slave_dsn = "dbi:mysql:host=127.0.0.1:port=".$slave_port.":user=root:database=".$database;
	my $slave_dbh = DBI->connect($slave_dsn, undef, undef, { RaiseError => 1 } );

	say("Establishing replication, mode $rpl_mode ...");

	my ($foo, $master_version) = $master_dbh->selectrow_array("SHOW VARIABLES LIKE 'version'");

	if (($master_version !~ m{^5\.0}sio) && ($rpl_mode ne 'default')) {
		$master_dbh->do("SET GLOBAL BINLOG_FORMAT = '$rpl_mode'");
		$slave_dbh->do("SET GLOBAL BINLOG_FORMAT = '$rpl_mode'");
	}

	$slave_dbh->do("STOP SLAVE");

	$slave_dbh->do("SET GLOBAL storage_engine = '$engine'") if defined $engine;

	$slave_dbh->do("CHANGE MASTER TO
		MASTER_PORT = $master_ports[0],
		MASTER_HOST = '127.0.0.1',
               MASTER_USER = 'root',
               MASTER_CONNECT_RETRY = 1
	");

	$slave_dbh->do("START SLAVE");
}

#
# Run actual queries
#

my @gentest_options;

push @gentest_options, "--start-dirty" if defined $start_dirty;
push @gentest_options, "--gendata=$gendata";
push @gentest_options, "--engine=$engine" if defined $engine;
push @gentest_options, "--rpl_mode=$rpl_mode" if defined $rpl_mode;
push @gentest_options, map {'--validator='.$_} split(/,/,$validators) if defined $validators;
push @gentest_options, map {'--reporter='.$_} split(/,/,$reporters) if defined $reporters;
push @gentest_options, "--threads=$threads" if defined $threads;
push @gentest_options, "--queries=$queries" if defined $queries;
push @gentest_options, "--duration=$duration" if defined $duration;
push @gentest_options, "--dsn=$master_dsns[0]" if defined $master_dsns[0];
push @gentest_options, "--dsn=$master_dsns[1]" if defined $master_dsns[1];
push @gentest_options, "--grammar=$grammar_file";
push @gentest_options, "--redefine=$redefine_file" if defined $redefine_file;
push @gentest_options, "--seed=$seed" if defined $seed;
push @gentest_options, "--mask=$mask" if defined $mask;
push @gentest_options, "--mask-level=$mask_level" if defined $mask_level;
push @gentest_options, "--rows=$rows" if defined $rows;
push @gentest_options, "--views" if defined $views;
push @gentest_options, "--varchar-length=$varchar_len" if defined $varchar_len;
push @gentest_options, "--xml-output=$xml_output" if defined $xml_output;
push @gentest_options, "--debug" if defined $debug;
push @gentest_options, "--filter=$filter" if defined $filter;
push @gentest_options, "--valgrind" if defined $valgrind;

# Push the number of "worker" threads into the environment.
# lib/GenTest/Generator/FromGrammar.pm will generate a corresponding grammar element.
$ENV{RQG_THREADS}= $threads;

my $gentest_result = system("perl $ENV{RQG_HOME}gentest.pl ".join(' ', @gentest_options));
say("gentest.pl exited with exit status ".($gentest_result >> 8));
exit_test($gentest_result >> 8) if $gentest_result > 0;

if ($rpl_mode) {
	my ($file, $pos) = $master_dbh->selectrow_array("SHOW MASTER STATUS");

	say("Waiting for slave to catch up..., file $file, pos $pos .");
	exit_test(STATUS_UNKNOWN_ERROR) if !defined $file;

	my $slave_dsn = "dbi:mysql:host=127.0.0.1:port=".$slave_port.":user=root:database=".$database;
	my $slave_dbh = DBI->connect($slave_dsn, undef, undef, { PrintError => 1 } );

	exit_test(STATUS_REPLICATION_FAILURE) if not defined $slave_dbh;

	$slave_dbh->do("START SLAVE");
	my $wait_result = $slave_dbh->selectrow_array("SELECT MASTER_POS_WAIT('$file',$pos)");

	if (not defined $wait_result) {
		say("MASTER_POS_WAIT() failed. Slave thread not running.");
		exit_test(STATUS_REPLICATION_FAILURE);
	}
}

#
# Compare master and slave, or two masters
#

if ($rpl_mode || (defined $basedirs[1])) {
	my @dump_ports = ($master_ports[0]);
	if ($rpl_mode) {
		push @dump_ports, $slave_port;
	} elsif (defined $basedirs[1]) {
		push @dump_ports, $master_ports[1];
	}

	my @dump_files;

	foreach my $i (0..$#dump_ports) {
		say("Dumping server on port $dump_ports[$i]...");
		$dump_files[$i] = tmpdir()."/server_".$$."_".$i.".dump";

		my $dump_result = system("\"$client_basedir/mysqldump\" --hex-blob --no-tablespaces --skip-triggers --compact --order-by-primary --skip-extended-insert --no-create-info --host=127.0.0.1 --port=$dump_ports[$i] --user=root $database | sort > $dump_files[$i]");
		exit_test($dump_result >> 8) if $dump_result > 0;
	}

	say("Comparing SQL dumps...");
	my $diff_result = system("diff -u $dump_files[0] $dump_files[1]");
	$diff_result = $diff_result >> 8;

	if ($diff_result == 0) {
		say("No differences were found between servers.");
	}

	foreach my $dump_file (@dump_files) {
		unlink($dump_file);
	}

	exit_test($diff_result);
}

sub help {

	print <<EOF
Copyright (c) 2008-2009 Sun Microsystems, Inc. All rights reserved. Use is subject to license terms.

$0 - Run a complete random query generation test, including server start with replication and master/slave verification
    
    Options related to one standalone MySQL server:

    --basedir   : Specifies the base directory of the stand-alone MySQL installation;
    --mysqld    : Options passed to the MySQL server
    --vardir    : Optional. (default \$basedir/mysql-test/var);

    Options related to two MySQL servers

    --basedir1  : Specifies the base directory of the first MySQL installation;
    --basedir2  : Specifies the base directory of the second MySQL installation;
    --mysqld1   : Options passed to the first MySQL server
    --mysqld2   : Options passed to the second MySQL server
    --vardir1   : Optional. (default \$basedir1/mysql-test/var);
    --vardir2   : Optional. (default \$basedir2/mysql-test/var);

    General options

    --grammar   : Grammar file to use when generating queries (REQUIRED);
    --redefine  : Grammar file to redefine and/or add rules to the given grammar
    --rpl_mode  : Replication type to use (statement|row|mixed) (default: no replication);
    --vardir1   : Optional.
    --vardir2   : Optional. 
    --engine    : Table engine to use when creating tables with gendata (default no ENGINE in CREATE TABLE);
    --threads   : Number of threads to spawn (default $default_threads);
    --queries   : Number of queries to execute per thread (default $default_queries);
    --duration  : Duration of the test in seconds (default $default_duration seconds);
    --validator : The validators to use
    --reporter  : The reporters to use
    --gendata   : Generate data option. Passed to gentest.pl
    --seed      : PRNG seed. Passed to gentest.pl
    --mask      : Grammar mask. Passed to gentest.pl
    --mask-level: Grammar mask level. Passed to gentest.pl
    --rows      : No of rows. Passed to gentest.pl
    --varchar-length: length of strings. passed to gentest.pl
    --xml-outputs: Passed to gentest.pl
    --views     : Generate views. Passed to gentest.pl
    --valgrind  : Passed to gentest.pl
    --filter    : Passed to gentest.pl
    --mem       : Passed to mtr.
    --mtr-build-thread: 
                  Value used for MTR_BUILD_THREAD when servers are started and accessed.
    --debug     : Debug mode
    --help      : This help message

    If you specify --basedir1 and --basedir2 or --vardir1 and --vardir2, two servers will be started and the results from the queries
    will be compared between them.
EOF
	;
	print "$0 arguments were: ".join(' ', @ARGV_saved)."\n";
	exit_test(STATUS_UNKNOWN_ERROR);
}

sub exit_test {
	my $status = shift;

	print localtime()." [$$] $0 will exit with exit status $status\n";
	safe_exit($status);
}
