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

$combinations = [
	['
		--grammar=conf/metadata_locking.yy
		--gendata=conf/metadata_locking.zz
		--queries=100K
		--duration=600
		--basedir=/build/bzr/azalea-bugfixing
		--validator=ResultsetProperties
                --reporters=Deadlock,ErrorLog,Backtrace,Shutdown
		--mysqld=--innodb-lock-wait-timeout=1
		--mysqld=--transaction-isolation=REPEATABLE-READ
	'], [
		'--engine=MyISAM',
		'--engine=MEMORY',
		'--engine=Innodb'
	], [
		'--rows=1',
		'--rows=10',
		'--rows=100',
	],
	[
		'--threads=4',
		'--threads=8',
		'--threads=16',
		'--threads=32',
		'--threads=64',
		'--threads=128'
	]
];
