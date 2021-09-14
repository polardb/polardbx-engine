#
# Random Query Generator tests
#
# Arguments needed to be set by the test when including this one:
#  $GRAMMAR_FILES: space separated list of grammar files
#  $DATA_FILE: name of the data file
#

let $MYSQL_BASEDIR = `SELECT @@BASEDIR`;
let RQG_BASE = suite/xengine_stress/rqg;
let TEST_BASE = suite/xengine_stress/t;
let MYSQL_SOCKET = `SELECT @@SOCKET`;
let GRAMMAR_FILES = $GRAMMAR_FILES;
let DATA_FILE = $DATA_FILE;
let TESTDB = $TARGET_DB;
let OTHER_ARGS = $OTHER_ARGS;
let TEST_QUERIES = $RQG_QUERIES;
let TEST_DURATION = $RQG_DURATION;

--disable_query_log
create user sbtest IDENTIFIED WITH mysql_native_password;
grant all privileges on *.* to 'sbtest'@'%';
--enable_query_log

--perl

foreach $grammar_file (split(/ /, $ENV{'GRAMMAR_FILES'})) {

  my $rqg_queries = $ENV{'TEST_QUERIES'} ? $ENV{'TEST_QUERIES'} : 2500;
  my $rqg_duration = $ENV{'TEST_DURATION'} ? $ENV{'TEST_DURATION'} : 600;

  # Errors from the gentest.pl file will be captured in the results file
  my $cmd = "RQG_HOME=$ENV{'RQG_BASE'} perl $ENV{'RQG_BASE'}/gentest.pl " .
      "--dsn=dbi:mysql:host=:port=:user=sbtest:database=$ENV{'TESTDB'}" .
      ":mysql_socket=$ENV{'MYSQL_SOCKET'} " .
      "--gendata=$ENV{'TEST_BASE'}/$ENV{'DATA_FILE'} " .
      "--grammar=$ENV{'TEST_BASE'}/$grammar_file " .
      "--threads=20 --queries=".$rqg_queries.
      " --duration=".$rqg_duration.
      " $ENV{'OTHER_ARGS'} --stage=1 --kill=0 2>&1 >> " .
      "$ENV{'MYSQLTEST_VARDIR'}/tmp/$ENV{'TESTDB'}.log";

  print "Running test with grammar file $grammar_file\n";
  system($cmd);
  if ($? != 0) {
    print ("Failure running test! Command executed: $cmd\n");
  } else {
    print ("rqg test success!\n");
    system("rm -f $ENV{'MYSQLTEST_VARDIR'}/tmp/$ENV{'TESTDB'}.log");
  }
}

EOF

--disable_query_log
drop user 'sbtest'@'%';
--enable_query_log

