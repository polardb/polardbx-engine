/* Copyright (c) 2007, 2017, Oracle and/or its affiliates. All rights reserved.

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

#include <assert.h>
#include <mysql.h>
#include <sys/types.h>

#include "m_string.h"
#include "my_compiler.h"
#include "my_sys.h"

int main(int argc, char **argv) {
  if (argc < 3) {
    printf("ERROR: para\n");
    return 1;
  }

  if (strcmp(argv[2], "fetchKey") == 0) {
    if (argc != 4) {
      printf("ERROR: para\n");
      return 1;
    }

    printf("b4fPuji+uxQieo8uKScgzWxV1DphwMxPGcxsFv64ywfKUd86ucWoI8SqF2ALB+vN6KLZk8sc3ZTbLzgI3KcbfSfPJqKX6cV8UPf7uOA3ZMfVJB7k9jSG516lJ97Aifkxss4ItgFNvHVkhCTMDQsT2wVkzAyyeLmDqcCqYzbWyGkVKXVZreWa5fcc2CJYHsbdAHmuCVW78fQMMecep7Mo72DN9y0zlcj8SSd2DL5J6F420dipwF2fGgfNhH7hiP8iHvHzVFzKM8V4gq9R5DzqJVvlguQwEEm1PJHtIge/2m8Yx/fVDT2kSWTCdSxp16YeUgQeMOK3LKI0YTXxTrhqUg==\n");
  } else {
    if (argc != 3) {
      printf("ERROR: para\n");
      return 1;
    }

    printf("INNODBKey-cbfc72a7-b739-40a4-95e7-84fbf9bf1e89-1\n");
  }

  return 0;
}

