# Copyright (C) 2008 Sun Microsystems, Inc. All rights reserved.
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
	insert | update | insert | delete | select;

insert:
	INSERT INTO _table ( _field , _field ) VALUES ( value , value );

select:
	SELECT _field FROM _table WHERE condition order_by ;

update:
 	UPDATE _table SET _field = digit WHERE condition order_by ;

delete:
	DELETE FROM _table WHERE condition LIMIT 1 ;

condition:
	_field IN ( value , value , value , value , value , value , value , value , value , value , value ) |
	_field sign value | _field sign value |
	_field BETWEEN value AND value |
	_field LIKE CONCAT( LEFT( value , _digit ) , '%' ) ;

sign:
	= | < | > | <> | <=> | != | >= | <= ;

value:
	_varchar(255);

order_by:
	|
	ORDER BY _field , _field ;
