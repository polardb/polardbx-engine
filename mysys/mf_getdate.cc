/* Copyright (c) 2000, 2017, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
  @file mysys/mf_getdate.cc
  Get date in a printable form: yyyy-mm-dd hh:mm:ss
*/

#include <m_string.h>
#include <stdio.h>
#include <time.h>

#include "my_sys.h"

/*
  get date as string

  SYNOPSIS
    get_date()
    to   - string where date will be written
    flag - format of date:
	  If flag & GETDATE_TIME	Return date and time
	  If flag & GETDATE_SHORT_DATE	Return short date format YYMMDD
	  If flag & GETDATE_HHMMSSTIME	Return time in HHMMDD format.
	  If flag & GETDATE_GMT		Date/time in GMT
	  If flag & GETDATE_FIXEDLENGTH	Return fixed length date/time
    date - for conversion
*/


MYSQL_PLUGIN_LEGACY_API void get_date(char * to, int flag, time_t date)
{
   struct tm *start_time;
   time_t skr;
  struct tm tm_tmp;

   skr=date ? (time_t) date : my_time(0);
   if (flag & GETDATE_GMT)
     gmtime_r(&skr,&tm_tmp);
   else
     localtime_r(&skr,&tm_tmp);
   start_time= &tm_tmp;
   if (flag & GETDATE_SHORT_DATE)
     sprintf(to,"%02d%02d%02d",
	     start_time->tm_year % 100,
	     start_time->tm_mon+1,
	     start_time->tm_mday);
   else
     sprintf(to, ((flag & GETDATE_FIXEDLENGTH) ?
		  "%4d-%02d-%02d" : "%d-%02d-%02d"),
	     start_time->tm_year+1900,
	     start_time->tm_mon+1,
	     start_time->tm_mday);
   if (flag & GETDATE_DATE_TIME)
     sprintf(strend(to),
	     ((flag & GETDATE_FIXEDLENGTH) ?
	      " %02d:%02d:%02d" : " %2d:%02d:%02d"),
	     start_time->tm_hour,
	     start_time->tm_min,
	     start_time->tm_sec);
   else if (flag & GETDATE_HHMMSSTIME)
     sprintf(strend(to),"%02d%02d%02d",
	     start_time->tm_hour,
	     start_time->tm_min,
	     start_time->tm_sec);
} /* get_date */
