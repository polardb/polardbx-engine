/* Copyright (c) 2014, 2022, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   51 Franklin Street, Suite 500, Boston, MA 02110-1335 USA */

#ifndef EVENT_CATALOGER_INCLUDE
#define EVENT_CATALOGER_INCLUDE

#include "applier.h"
#include <mysql/group_replication_priv.h>

class Event_cataloger : public Event_handler
{
public:
  Event_cataloger();
  int handle_event(Pipeline_event *ev,Continuation *cont);
  int handle_action(Pipeline_action *action);
  int initialize();
  int terminate();
  bool is_unique();
  int get_role();
};

#endif /* EVENT_CATALOGER_INCLUDE */
