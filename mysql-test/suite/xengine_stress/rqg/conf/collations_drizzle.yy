# Copyright (C) 2010 Sun Microsystems, Inc. All rights reserved.
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

query:
	delete | insert | update ;

xid_event:
	START TRANSACTION | COMMIT ;
insert:
	INSERT INTO table_name ( field_name ) VALUES ( ' letter ' ) ;

update:
	UPDATE table_name ' letter ' WHERE field_name oper ' letter ';

delete:
	DELETE FROM table_name WHERE field_name oper ' letter ';

table_name:
	_table ;

field_name:
	`col_varchar_key` | `col_varchar_nokey` ;

oper:
	= | > | < | >= | <= | <> ;
