#!/usr/bin/perl

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
use strict;
use Carp;
use Data::Dumper;
use IPC::ShareLite;
use IPC::SysV qw(S_IRUSR S_IWUSR IPC_CREAT IPC_NOWAIT SEM_UNDO);
use IPC::Semaphore;

use GenTest;
use GenTest::Properties;
use GenTest::Constants;
use GenTest::App::Gendata;
use GenTest::App::GendataSimple;
use GenTest::IPC::Channel;
use GenTest::IPC::Process;
use GenTest::ErrorFilter;
use GenTest::Controller;
use GenTest::Controller::KillRandom;
use GenTest::Controller::Stage;

$| = 1;
my $ctrl_c = 0;

$SIG{INT} = sub { $ctrl_c = 1 };
$SIG{TERM} = sub { exit(0) };
$SIG{CHLD} = "IGNORE" if windows();

if (defined $ENV{RQG_HOME}) {
	$ENV{RQG_HOME} = windows() ? $ENV{RQG_HOME}.'\\' : $ENV{RQG_HOME}.'/';
}

use constant PROCESS_TYPE_PARENT	=> 0;
use constant PROCESS_TYPE_PERIODIC	=> 1;
use constant PROCESS_TYPE_CHILD		=> 2;
use constant PROCESS_TYPE_CONTROLLER => 3;
use constant PROCESS_TYPE_STAGE => 4;
use constant PROCESS_TYPE_KILL => 5;

# use POSIX;
use Getopt::Long;
use Time::HiRes;

use GenTest::XML::Report;
use GenTest::XML::Test;
use GenTest::XML::BuildInfo;
use GenTest::Constants;
use GenTest::Result;
use GenTest::Validator;
use GenTest::Generator::FromGrammar;
use GenTest::Executor;
use GenTest::Mixer;
use GenTest::Reporter;
use GenTest::ReporterManager;
use GenTest::Filter::Regexp;

my $DEFAULT_THREADS = 10;
my $DEFAULT_QUERIES = 1000;
my $DEFAULT_DURATION = 3600;
my $DEFAULT_DSN = 'dbi:mysql:host=127.0.0.1:port=9306:user=root:database=test';

my @ARGV_saved = @ARGV;

my $options = {};

my $shutdown = IPC::ShareLite->new(
    -key     => "shut",
    -create  => 'yes',
    -destroy => 'no'
) or die $!;

my $kill_random_sem;

my $opt_result = GetOptions($options,
                            'config=s',
                            'dsn=s@',
                            'dsn1=s',
                            'dsn2=s',
                            'dsn3=s',
                            'engine=s',
                            'gendata:s',
                            'grammar=s',
                            'redefine=s',
                            'threads=i',
                            'queries=s',
                            'duration=s',
                            'help',
                            'debug',
                            'rpl_mode=s',
                            'validators:s@',
                            'reporters:s@',
                            'seed=s',
                            'mask=i',
                            'mask-level=i',
                            'rows=i',
                            'varchar-length=i',
                            'xml_output=s',
                            'views',
                            'start-dirty',
                            'filter=s',
                            'stage=i',
                            'kill=i',
                            'errorlevel=s',
                            'mycnf=s',
                            'valgrind');
backwardCompatability($options);
my $config = GenTest::Properties->new(
    options => $options,
    defaults => {dsn=>[$DEFAULT_DSN],
                 seed => 1,
                 queries => $DEFAULT_QUERIES,
                 duration => $DEFAULT_DURATION,
                 threads => $DEFAULT_THREADS},
    required => ['grammar'],
    legal => ['dsn',
              'engine',
              'gendata',
              'redefine',
              'threads',
              'queries',
              'duration',
              'help',
              'debug',
              'rpl_mode',
              'validators',
              'reporters',
              'seed',
              'mask',
              'mask-level',
              'rows',
              'varchar-length',
              'xml-output',
              'views',
              'start-dirty',
              'filter',
              'valgrind',
              'stage',
              'kill',
              'mycnf',
              'errorlevel'],
    help => \&help);

my $seed = $config->seed;
if ($seed eq 'time') {
	$seed = time();
	say("Converting --seed=time to --seed=$seed");
}
my $failure_level = STATUS_OK;
if ($config->errorlevel eq 'CRITICAL') {
  $failure_level = STATUS_CRITICAL_FAILURE;
}
if ($config->errorlevel eq 'FAILURE') {
  $failure_level = STATUS_TEST_FAILURE;
}

$ENV{RQG_DEBUG} = 1 if $config->debug;

my $queries = $config->queries;
$queries =~ s{K}{000}so;
$queries =~ s{M}{000000}so;

help() if !$opt_result || $config->help;

say("Starting \n $0 \\ \n ".join(" \\ \n ", @ARGV_saved));

say("-------------------------------\nConfiguration");
$config->printProps;

if ((defined $config->gendata) && 
    (not defined $config->property('start-dirty'))) {
	foreach my $dsn (@{$config->dsn}) {
		next if $dsn eq '';
		my $gendata_result;
        my $datagen;
		if ($config->gendata eq '') {
            $datagen = GenTest::App::GendataSimple->new(dsn => $dsn,
                                                        views => $config->views,
                                                        engine => $config->engine);
		} else {
            $datagen = GenTest::App::Gendata->new(spec_file => $config->gendata,
                                                  dsn => $dsn,
                                                  engine => $config->engine,
                                                  seed => $seed,
                                                  debug => $config->debug,
                                                  rows => $config->rows,
                                                  views => $config->views,
                                                  varchar_length => $config->property('varchar-length'));
		}
    $gendata_result = $datagen->run();
		safe_exit ($gendata_result >> 8) if $gendata_result > STATUS_OK;
	}
}

my $test_start = time();
my $test_end = $test_start + $config->duration;

my $grammar = GenTest::Grammar->new(
	grammar_file => $config->grammar
    );

exit(STATUS_ENVIRONMENT_FAILURE) if not defined $grammar;

if (defined $config->redefine) {
    my $patch_grammar = GenTest::Grammar->new(
        grammar_file => $config->redefine);
    $grammar = $grammar->patch($patch_grammar);
}

exit(STATUS_ENVIRONMENT_FAILURE) if not defined $grammar;

my $channel = GenTest::IPC::Channel->new();

my @executors;
foreach my $i (0..2) {
	next if $config->dsn->[$i] eq '';
	push @executors, GenTest::Executor->newFromDSN($config->dsn->[$i],
                                                   windows()?undef:$channel);
}

my $drizzle_only = $executors[0]->type == DB_DRIZZLE;
$drizzle_only = $drizzle_only && $executors[1]->type == DB_DRIZZLE if $#executors > 0;

my $mysql_only = $executors[0]->type == DB_MYSQL;
$mysql_only = $mysql_only && $executors[1]->type == DB_MYSQL if $#executors > 0;

if (not defined $config->reporters or $#{$config->reporters} < 0) {
    $config->reporters([]);
	if ($mysql_only || $drizzle_only) {
		$config->reporters(['ErrorLog', 'Backtrace']);
	}
} else {
    ## Remove the "None" reporter
    foreach my $i (0..$#{$config->reporters}) {
        delete $config->reporters->[$i] 
            if $config->reporters->[$i] eq "None" 
            or $config->reporters->[$i] eq '';
    }
}

say("Reporters: ".($#{$config->reporters} > -1 ? join(', ', @{$config->reporters}) : "(none)"));

my $reporter_manager = GenTest::ReporterManager->new();

if ($mysql_only ) {
	foreach my $i (0..2) {
		next if $config->dsn->[$i] eq '';
		foreach my $reporter (@{$config->reporters}) {
			my $add_result = $reporter_manager->addReporter($reporter, {
				dsn			=> $config->dsn->[$i],
				test_start	=> $test_start,
				test_end	=> $test_end,
				test_duration	=> $config->duration
															} );
			exit($add_result) if $add_result > STATUS_OK;
		}
	}
}

if (not defined $config->validators or $#{$config->validators} < 0) {
    $config->validators([]);
	push(@{$config->validators}, 'ErrorMessageCorruption') 
        if ($mysql_only || $drizzle_only);
    if ($config->dsn->[2] ne '') {
		push @{$config->validators}, 'ResultsetComparator3';
    } elsif ($config->dsn->[1] ne '') {
		push @{$config->validators}, 'ResultsetComparator';
    }

    push @{$config->validators}, 'ReplicationSlaveStatus' 
        if $config->rpl_mode ne '' && ($mysql_only || $drizzle_only);
    push @{$config->validators}, 'MarkErrorLog' 
        if (defined $config->valgrind) && ($mysql_only || $drizzle_only);

    push @{$config->validators}, 'QueryProperties' 
        if $grammar->hasProperties() && ($mysql_only || $drizzle_only);
} else {
    ## Remove the "None" validator
    foreach my $i (0..$#{$config->validators}) {
        delete $config->validators->[$i] 
            if $config->validators->[$i] eq "None"
            or $config->validators->[$i] eq '';
    }
}
say("Validators: ".($config->validators and $#{$config->validators} > -1 ? join(', ', @{$config->validators}) : "(none)"));

my $filter_obj;

$filter_obj = GenTest::Filter::Regexp->new( file => $config->filter ) 
    if defined $config->filter;

say("Starting ".$config->threads." processes, ".
    $config->queries." queries each, duration ".
    $config->duration." seconds.");

my $buildinfo;
if (defined $config->property('xml-output')) {
	$buildinfo = GenTest::XML::BuildInfo->new(
		dsns => $config->dsn
		);
}

my $test = GenTest::XML::Test->new(
	id => Time::HiRes::time(),
	attributes => {
		engine => $config->engine,
		gendata => $config->gendata,
		grammar => $config->grammar,
		threads => $config->threads,
		queries => $config->queries,
		validators => join (',', @{$config->validators}),
		reporters => join (',', @{$config->reporters}),
		seed => $seed,
		mask => $config->mask,
		mask_level => $config->property('mask-level'),
		rows => $config->rows,
		'varchar-length' => $config->property('varchar-length')
	}
);

my $report = GenTest::XML::Report->new(
	buildinfo => $buildinfo,
	tests => [ $test ]
);

my $errorfilter = GenTest::ErrorFilter->new(channel=>$channel);
my $errorfilter_p = GenTest::IPC::Process->new(object=>$errorfilter);
if (!windows()) {
    $errorfilter_p->start();
}

my $process_type;
my %child_pids;
my $id = 1;

my $periodic_pid = fork();

# init some controllers even if we don't need.
my $controller_pid;

my $kill_random = GenTest::Controller::KillRandom->new((
  dsn	=> $config->dsn->[0],
  test_start => $test_start,
  test_end => $test_end,
  test_duration	=> $config->duration,
  grammar => $grammar,
));

$shutdown->store(0);

say('thread: '.$config->threads);

$kill_random_sem = IPC::Semaphore->new('kill_random_sem',
    $config->threads,
    S_IRUSR | S_IWUSR | IPC_CREAT);
say("semaphore id ".$kill_random_sem->id);
$kill_random_sem->setall((1) x ($config->threads));

$kill_random->init($kill_random_sem, $config->threads, $config->mycnf);


my $stage_roller = GenTest::Controller::Stage->new((
  dsn	=> $config->dsn->[0],
  test_start => $test_start,
  test_end => $test_end,
  test_duration	=> $config->duration,
  grammar => $grammar,
));

my $stage_pid;

if ($periodic_pid == 0) {
	Time::HiRes::sleep(($config->threads + 1) / 10);
	say("Started periodic reporting process...");
	$process_type = PROCESS_TYPE_PERIODIC;
	$id = 0;
  $kill_random_sem->remove;
} else {
	foreach my $i (1..$config->threads) {
		my $child_pid = fork();
        $channel->writer;
		if ($child_pid == 0) { # This is a child
			$process_type = PROCESS_TYPE_CHILD;
			last;
		} else {
			$child_pids{$child_pid} = 1;
			$process_type = PROCESS_TYPE_PARENT;
			$seed++;
			$id++;
			Time::HiRes::sleep(0.1);	# fork slowly for more predictability
			next;
		}
	}
  if ($process_type == PROCESS_TYPE_PARENT) {
    foreach my $i (1..2) {
      if ($i == 1) {
        $stage_pid = fork();
        if ($stage_pid == 0) {
          $process_type = PROCESS_TYPE_STAGE;
          if (not $config->stage) {
            exit(0);
          }
          $stage_roller->init($config);
          say("Started ".$i." stage process ".$$." ...");
          last;
        }
      } else {
        $controller_pid = fork();
        if ($controller_pid == 0) {
          $process_type = PROCESS_TYPE_KILL;
          if (not $config->kill) {
            exit(0);
          }
          say("Started ".$i." kill process ...");
          last;
        }
      }
    }
  }
}

if ($process_type == PROCESS_TYPE_PARENT) {
  if (windows()) {
      ## Important that this is done here in the parent after the last
      ## fork since on windows Process.pm uses threads
      $errorfilter_p->start();
  }
	# We are the parent process, wait for for all spawned processes to terminate
	my $children_died = 0;
	my $total_status = STATUS_OK;
	my $periodic_died = 0;

  ## Parent thread does not use channel
  $channel->close;

	while (1) {
		my $child_pid = wait();
		my $exit_status = $? > 0 ? ($? >> 8) : 0;

		$total_status = $exit_status if $exit_status > $total_status;

		if ($child_pid == $periodic_pid) {
			$periodic_died = 1;
			last;
		} else {
			$children_died++;
			delete $child_pids{$child_pid};
		}

		last if $exit_status >= STATUS_CRITICAL_FAILURE;
		last if $children_died == $config->threads;
		last if $child_pid == -1;
	}

  $kill_random_sem->remove;
	foreach my $child_pid (keys %child_pids) {
		say("Killing child process with pid $child_pid...");
    say('kill15 '.$child_pid);
		kill(15, $child_pid);
	}

	if ($periodic_died == 0) {
		# Wait for periodic process to return the status of its last execution
		Time::HiRes::sleep(1);
		say("Killing periodic reporting process with pid $periodic_pid...");
    say('kill15 '.$periodic_pid);
		kill(15, $periodic_pid);

		if (windows()) {
			# We use sleep() + non-blocking waitpid() due to a bug in ActiveState Perl
			Time::HiRes::sleep(1);
			waitpid($periodic_pid, &POSIX::WNOHANG() );
		} else {
			waitpid($periodic_pid, 0);
		}

		if ($? > -1 ) {
			my $periodic_status = $? > 0 ? $? >> 8 : 0;
			$total_status = $periodic_status if $periodic_status > $total_status;
		}
	}

  say('kill9 '.$controller_pid);
  kill(9, $controller_pid);
  say('kill9 '.$stage_pid);
  kill(9, $stage_pid);
  $errorfilter_p->kill();

	my @report_results;

	if ($total_status == STATUS_OK) {
		@report_results = $reporter_manager->report(REPORTER_TYPE_SUCCESS | REPORTER_TYPE_ALWAYS);
	} elsif (
		($total_status == STATUS_LENGTH_MISMATCH) ||
		($total_status == STATUS_CONTENT_MISMATCH)
	) {
		@report_results = $reporter_manager->report(REPORTER_TYPE_DATA);
	} elsif ($total_status == STATUS_SERVER_CRASHED) {
		say("Server crash reported, initiating post-crash analysis...");
		@report_results = $reporter_manager->report(REPORTER_TYPE_CRASH | REPORTER_TYPE_ALWAYS);
	} elsif ($total_status == STATUS_SERVER_DEADLOCKED) {
		say("Server deadlock reported, initiating analysis...");
		@report_results = $reporter_manager->report(REPORTER_TYPE_DEADLOCK | REPORTER_TYPE_ALWAYS);
	} elsif ($total_status == STATUS_SERVER_KILLED) {
		@report_results = $reporter_manager->report(REPORTER_TYPE_SERVER_KILLED | REPORTER_TYPE_ALWAYS);
	} else {
		@report_results = $reporter_manager->report(REPORTER_TYPE_ALWAYS);
	}

	my $report_status = shift @report_results;
	$total_status = $report_status if $report_status > $total_status;
	$total_status = STATUS_OK if $total_status == STATUS_SERVER_KILLED;

	foreach my $incident (@report_results) {
		$test->addIncident($incident);
	}

	$test->end($total_status == STATUS_OK ? "pass" : "fail");

	if (defined $config->property('xml-output')) {
		open (XML , ">$config->property('xml-output')") or say("Unable to open $config->property('xml-output'): $!");
		print XML $report->xml();
		close XML;
	}

  $kill_random_sem->remove;
	if ($total_status == STATUS_OK) {
		say("Test completed successfully.");
		safe_exit(0);
	} else {
		say("Test completed with failure status $total_status.");
		safe_exit($total_status);
	}
} elsif ($process_type == PROCESS_TYPE_PERIODIC) {
    ## Periodic does not use channel
    $channel->close();
	while (1) {
		my $reporter_status = $reporter_manager->monitor(REPORTER_TYPE_PERIODIC);
		exit($reporter_status) if $reporter_status > STATUS_CRITICAL_FAILURE;
    sleep(10);
    # $controller->monitor(REPORTER_TYPE_PERIODIC);
    # $controller->report(REPORTER_TYPE_SERVER_KILLED);
	}
  $kill_random_sem->remove;
} elsif ($process_type == PROCESS_TYPE_STAGE) {
  while (1) {
    sleep(10);
    say('stage roller start to run');
    my $thread_index = $config->threads - 1;
    while ($thread_index >= 0) {
      my $res = $kill_random_sem->op($thread_index, -1, IPC_NOWAIT);
      last if $ctrl_c;
      next if not $res;
      $thread_index -= 1;
    }
    if ($ctrl_c == 1) {
      $kill_random_sem->remove();
      last;
    }
    my $roller_res = $stage_roller->run();
    if ($roller_res > STATUS_OK) {
      say('exit stage roller2 '.$roller_res);
      exit($roller_res);
    }
    $thread_index = $config->threads - 1;
    while ($thread_index >= 0) {
      $kill_random_sem->op($thread_index, 1, SEM_UNDO);
      $thread_index -= 1;
    }
    if ($ctrl_c == 1) {
      $kill_random_sem->remove();
      last;
    }
  }
  $kill_random_sem->remove;
} elsif ($process_type == PROCESS_TYPE_KILL) {
  while (1) {
    sleep(10);
    $kill_random->run();
    if ($ctrl_c == 1) {
      last;
    }
  }
  $kill_random_sem->remove;
} elsif ($process_type == PROCESS_TYPE_CHILD) {
	# We are a child process, execute the desired queries and terminate

	my $generator = GenTest::Generator::FromGrammar->new(
		grammar => $grammar,
		varchar_length => $config->property('varchar-length'),
		seed => $seed + $id,
		thread_id => $id,
		mask => $config->mask,
	        mask_level => $config->property('mask-level')
	);

	exit (STATUS_ENVIRONMENT_FAILURE) if not defined $generator;

	my $mixer = GenTest::Mixer->new(
		generator => $generator,
		executors => \@executors,
		validators => $config->validators,
		filters => defined $filter_obj ? [ $filter_obj ] : undef,
    group_checkers => {}
	);

	exit (STATUS_ENVIRONMENT_FAILURE) if not defined $mixer;

	my $max_result = 0;

	foreach my $i (1..$queries) {
    foreach my $executor (@executors) {
      $executor->execute("ROLLBACK;");
    }
    while (1) {
      my $res = $kill_random_sem->op($id-1, -1, IPC_NOWAIT);
		  last if $ctrl_c == 1;
      last if $res;
    }
		last if $ctrl_c == 1;
		my $result = $mixer->next();
    $kill_random_sem->op($id-1, 1, 0);
    if ($result > $failure_level) {
      say("mixer status: ".$result." and ".$failure_level);
    }
		exit($result) if $result > $failure_level;
		$max_result = $result if $result > $max_result && $result > STATUS_TEST_FAILURE;
		last if $result == STATUS_EOF;
		last if $ctrl_c == 1;
		last if time() > $test_end;
	}

	if ($max_result > 0) {
		say("Child process completed with error code $max_result.");
		exit($max_result);
	} else {
		say("Child process completed successfully.");
		exit(0);
	}

  $kill_random_sem->remove;
} else {
	croak ("Unknown process type $process_type");
}

sub help {

    print <<EOF
$0 - Testing via random query generation. Options:

        --dsn      : DBI resources to connect to (default $DEFAULT_DSN).
                      Supported databases are MySQL, Drizzle, PostgreSQL, JavaDB
                      first --dsn must be to MySQL or Drizzle
        --gendata   : Execute gendata-old.pl in order to populate tables with simple data (default NO)
        --gendata=s : Execute gendata.pl in order to populate tables with data 
                      using the argument as specification file to gendata.pl
        --engine    : Table engine to use when creating tables with gendata (default: no ENGINE for CREATE TABLE)
        --threads   : Number of threads to spawn (default $DEFAULT_THREADS)
        --queries   : Numer of queries to execute per thread (default $DEFAULT_QUERIES);
        --duration  : Duration of the test in seconds (default $DEFAULT_DURATION seconds);
        --grammar   : Grammar file to use for generating the queries (REQUIRED);
        --redefine  : Grammar file to redefine and/or add rules to the given grammar
        --seed      : PRNG seed (default 1). If --seed=time, the current time will be used.
        --rpl_mode  : Replication mode
        --validator : Validator classes to be used. Defaults
                           ErrorMessageCorruption if one or two MySQL dsns
                           ResultsetComparator3 if 3 dsns
                           ResultsetComparartor if 2 dsns
        --reporter  : ErrorLog, Backtrace if one or two MySQL dsns
        --mask      : A seed to a random mask used to mask (reduce) the grammar.
        --mask-level: How many levels deep the mask is applied (default 1)
        --rows      : Number of rows to generate for each table in gendata.pl, unless specified in the ZZ file
        --varchar-length: maximum length of strings (deault 1) in gendata.pl
        --views     : Pass --views to gendata-old.pl or gendata.pl
        --filter    : ......
        --start-dirty: Do not generate data (use existing database(s))
        --xml-output: ......
        --valgrind  : ......
        --filter    : ......
        --help      : This help message
        --debug     : Provide debug output
EOF
	;
	safe_exit(1);
}

sub backwardCompatability {
    my ($options) = @_;
    if (defined $options->{dsn}) {
        croak ("Do not combine --dsn and --dsnX")
            if defined $options->{dsn1} or
            defined $options->{dsn2} or
            defined $options->{dsn3};

    } else {
        my @dsns;
        foreach my $i (1..3) {
            if (defined $options->{'dsn'.$i}) {
                push @dsns, $options->{'dsn'.$i};
                delete $options->{'dsn'.$i};
            }
        }
        $options->{dsn} = \@dsns;
    }

    if (grep (/,/,@{$options->{reporters}})) {
        my $newreporters = [];
        map {push(@$newreporters,split(/,/,$_))} @{$options->{reporters}};
        $options->{reporters}=$newreporters ;
    }
    if (grep (/,/,@{$options->{validators}})) {
        my $newvalidators = [];
        map {push(@$newvalidators,split(/,/,$_))} @{$options->{validators}};
        $options->{validators}=$newvalidators ;
    }
}

