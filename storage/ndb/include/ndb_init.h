/*
   Copyright (C) 2004-2006 MySQL AB
    All rights reserved. Use is subject to license terms.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/


#ifndef NDB_INIT_H
#define NDB_INIT_H

#include <ndb_types.h>

#ifdef  __cplusplus
extern "C" {
#endif
/* call in main() - does not return on error */
extern NDB_EXPORT int ndb_init(void);
extern NDB_EXPORT void ndb_end(int);
#define NDB_INIT(prog_name) {my_progname=(prog_name); ndb_init();}
#ifdef  __cplusplus
}
#endif

#endif
