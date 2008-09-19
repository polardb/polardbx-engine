/* Copyright (C) 2003 MySQL AB

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

#ifndef CREATE_TRIG_IMPL_HPP
#define CREATE_TRIG_IMPL_HPP

#include "SignalData.hpp"
#include <Bitmask.hpp>
#include <AttributeList.hpp>

struct CreateTrigImplReq
{
  enum RequestType
  {
    CreateTriggerOnline = 1,
    CreateTriggerOffline = 2,
  };

  STATIC_CONST( SignalLength = 11 + MAXNROFATTRIBUTESINWORDS);
  SECTION( TRIGGER_NAME_SECTION = 0 );
  SECTION( ATTRIBUTE_MASK_SECTION = 1 );        // not yet in use

  // tableVersion, indexVersion, name section used only within DICT

  Uint32 senderRef;
  Uint32 senderData;
  Uint32 requestType;
  Uint32 tableId;
  Uint32 tableVersion;
  Uint32 indexId;
  Uint32 indexVersion;
  Uint32 triggerNo;
  Uint32 triggerId;
  Uint32 triggerInfo;
  Uint32 receiverRef;
  AttributeMask attributeMask;
};

struct CreateTrigImplConf {
  STATIC_CONST( SignalLength = 5 );

  Uint32 senderRef;
  Uint32 senderData;
  Uint32 tableId;       // BACKUP and SUMA want these back from TUP
  Uint32 triggerId;
  Uint32 triggerInfo;
};

struct CreateTrigImplRef {
  enum ErrorCode {
    NoError = 0,
    Busy = 701,
    NotMaster = 702,
    TriggerNameTooLong = 4236,
    TooManyTriggers = 4237,
    TriggerNotFound = 4238,
    TriggerExists = 4239,
    UnsupportedTriggerType = 4240,
    BadRequestType = 4247,
    InvalidName = 4248,
    InvalidTable = 4249,
    InconsistentTC = 293
  };

  STATIC_CONST( SignalLength = 9 );

  Uint32 senderRef;
  Uint32 senderData;
  Uint32 tableId;
  Uint32 triggerId;
  Uint32 triggerInfo;
  Uint32 errorCode;
  Uint32 errorLine;
  Uint32 errorNodeId;
  Uint32 masterNodeId;
};

#endif
