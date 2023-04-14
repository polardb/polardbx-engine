/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef HANDLER_EXT_INCLUDED
#define HANDLER_EXT_INCLUDED

#include "lizard_iface.h"
#include "my_inttypes.h"

typedef my_gcn_t (*load_gcn_t)();
typedef my_scn_t (*load_scn_t)();

/**
  The extension of handlerton struct.
*/
struct handlerton_ext {
  load_gcn_t load_gcn;
  load_scn_t load_scn;
};

/**
  load gcn from storage engine(s)

*/
my_gcn_t ha_load_gcn();
my_scn_t ha_load_scn();

#endif
