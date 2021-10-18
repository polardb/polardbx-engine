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
use lib 'lib';
use lib '../lib';
use DBI;

use GenTest::Constants;
use GenTest::Executor::MySQL;
use GenTest::Simplifier::SQL;
use GenTest::Simplifier::Test;

#
# Please modify those settings to fit your environment before you run this script
#

my $basedir = '/build/bzr/mysql-next-mr-bugfixing';
my $vardir = '/build/bzr/mysql-next-mr-bugfixing/mysql-test/var';
my $dsn = 'dbi:mysql:host=127.0.0.1:port=19306:user=root:database=test';
my $original_query = "
SELECT    table5 . `pk` AS field1 , table2 . `pk` AS field2 , table3 . `pk` AS field3 ,
table4 . `pk` AS field4 , table1 . `int_key` AS field5 , table1 . `int_key` AS field6 ,
COUNT( table1 . `pk` ) AS field7 FROM  BB AS table1  LEFT  JOIN    CC AS table2  LEFT 
JOIN  CC AS table3  LEFT  JOIN  CC AS table4  RIGHT  JOIN D AS table5 ON  table4 . `pk` =
 table5 . `pk`  ON  table3 . `int_key` =  table4 . `int_key`  ON  table2 . `int_key` = 
table4 . `pk`   LEFT  JOIN   CC AS table6  LEFT  JOIN  C AS table7  LEFT  JOIN B AS
table8 ON  table7 . `pk` =  table8 . `pk`  ON  table6 . `int_key` =  table7 . `pk`  
RIGHT  JOIN  C AS table9  LEFT  JOIN C AS table10 ON  table9 . `int_key` =  table10 .
`int_key`  ON  table6 . `int_key` =  table9 . `pk`  ON  table5 . `int_key` =  table9 .
`pk`   RIGHT  JOIN  C AS table11  LEFT  JOIN  CC AS table12  LEFT  JOIN  B AS table13 
LEFT OUTER JOIN D AS table14 ON  table13 . `int_key` =  table14 . `int_key`  ON  table12
. `pk` =  table14 . `int_key`  ON  table11 . `int_key` =  table12 . `pk`  ON  table9 .
`int_key` =  table11 . `int_key`  ON  table1 . `pk` =  table12 . `pk`  WHERE ( table4 .
`pk` >= table1 . `pk` OR table1 . `int_nokey` <> table1 . `pk` )  GROUP BY field1,
field2, field3, field4, field5, field6 HAVING field4 < 1 ORDER BY field2 ASC , field4
";
# Maximum number of seconds a query will be allowed to proceed. It is assumed that most crashes will happen immediately after takeoff
my $timeout = 2;

my @mtr_options = (
	'--start-and-exit',
	'--start-dirty',
	"--vardir=$vardir",
	"--master_port=19306",
	'--skip-ndbcluster',
	'1st'	# Required for proper operation of MTR --start-and-exit
);

my $orig_database = 'test';
my $new_database = 'crash';

my $executor;
start_server();

my $simplifier = GenTest::Simplifier::SQL->new(
	oracle => sub {
		my $oracle_query = shift;
		my $dbh = $executor->dbh();
	
		my $connection_id = $dbh->selectrow_array("SELECT CONNECTION_ID()");
		$dbh->do("CREATE EVENT timeout ON SCHEDULE AT CURRENT_TIMESTAMP + INTERVAL $timeout SECOND DO KILL QUERY $connection_id");

		my $oracle_result = $executor->execute($oracle_query);

		$dbh->do("DROP EVENT IF EXISTS timeout");

		if (!$executor->dbh()->ping()) {
			start_server();
			return ORACLE_ISSUE_STILL_REPEATABLE;
		} else {
			return ORACLE_ISSUE_NO_LONGER_REPEATABLE;
		}
	}
);

my $simplified_query = $simplifier->simplify($original_query);
print "Simplified query:\n$simplified_query;\n\n";

my $simplifier_test = GenTest::Simplifier::Test->new(
	executors => [ $executor ],
	queries => [ $simplified_query , $original_query ]
);

my $simplified_test = $simplifier_test->simplify();

print "Simplified test\n\n";
print $simplified_test;

sub start_server {
	chdir($basedir.'/mysql-test') or die $!;
	system("MTR_VERSION=1 perl mysql-test-run.pl ".join(" ", @mtr_options));

	$executor = GenTest::Executor::MySQL->new( dsn => $dsn );

	$executor->init();

	my $dbh = $executor->dbh();

	$dbh->do("SET GLOBAL EVENT_SCHEDULER = ON");
}
