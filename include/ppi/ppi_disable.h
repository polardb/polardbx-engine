/* Copyright (c) 2008, 2018, Alibaba and/or its affiliates. All rights reserved.

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
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef PPI_PPI_DISABLE_H_INCLUDED
#define PPI_PPI_DISABLE_H_INCLUDED

/**
  Interface of disabling PPI
*/
class Disable_ppi_iface {
 public:
  virtual ~Disable_ppi_iface() = default;
  virtual bool disable_ppi_statement_stat() = 0;
  virtual bool disable_ppi_thread_stat() = 0;
  virtual bool disable_ppi_transaction_stat() = 0;
};

class Disable_ppi_statement : public Disable_ppi_iface {
 public:
  bool disable_ppi_statement_stat() override { return true; }
  bool disable_ppi_thread_stat() override { return false; }
  bool disable_ppi_transaction_stat() override { return false; }
};

class Disable_ppi_thread : public Disable_ppi_iface {
 public:
  bool disable_ppi_statement_stat() override { return false; }
  bool disable_ppi_thread_stat() override { return true; }
  bool disable_ppi_transaction_stat() override { return false; }
};

class Disable_ppi_transaction : public Disable_ppi_iface {
 public:
  bool disable_ppi_statement_stat() override { return false; }
  bool disable_ppi_thread_stat() override { return false; }
  bool disable_ppi_transaction_stat() override { return true; }
};

class Disable_ppi_all : public Disable_ppi_iface {
  bool disable_ppi_statement_stat() override { return true; }
  bool disable_ppi_thread_stat() override { return true; }
  bool disable_ppi_transaction_stat() override { return true; }
};

static Disable_ppi_statement disable_statement;
static Disable_ppi_thread disable_thread;
static Disable_ppi_transaction disable_transaction;
static Disable_ppi_all disable_all;
#endif
