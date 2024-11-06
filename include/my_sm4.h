/* Copyright (c) 2000, 2018, Alibaba and/or its affiliates. All rights reserved.
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

#ifndef _MY_SM4_H
#define _MY_SM4_H

#include "my_inttypes.h"

enum my_sm4_opmode { SM4_CTR, SM4_ECB };

/**
  Encrypt a buffer using SM4

  @param [in] source           Pointer to data for encryption
  @param [in] source_length    Size of encryption data
  @param [out] dest            Buffer to place encrypted data
  @param [in] key              Key to be used for encryption
  @param [in] key_length       Length of the key
  @param [in] mode             encryption mode
  @param [in] iv               16 bytes initialization vector if needed.
                               Otherwise NULL
  @return size of encrypted data, or negative in case of error
*/
int my_sm4_encrypt(const unsigned char *source, uint32 source_length,
                   unsigned char *dest, const unsigned char *key,
                   uint32 key_length, enum my_sm4_opmode mode,
                   const unsigned char *iv);

/**
  Decrypt an SM4 encrypted buffer

  @param [in] source         Pointer to data for decryption
  @param [in] source_length  size of encrypted data
  @param [out]dest           buffer to place decrypted data
  @param [in] key            Key to be used for decryption
  @param [in] key_length     Length of the key
  @param [in] mode           encryption mode
  @param [in] iv             16 bytes initialization vector if needed.
                             Otherwise NULL
  @return size of original data.
*/
int my_sm4_decrypt(const unsigned char *source, uint32 source_length,
                   unsigned char *dest, const unsigned char *key,
                   uint32 key_length, enum my_sm4_opmode mode,
                   const unsigned char *iv);

/**
  Generate encrypt key and iv.

  @param [in] passwd     Input password
  @param [in] passwd_len Size of input password
  @param [out]key        16 bytes encrypt key
  @param [out]iv         16 bytes initialization vector
                        Otherwise NULL
  @return 0 if succeeded; otherwise error occurs
*/
int my_sm4_generate_key_iv(unsigned char *passwd, uint32 passwd_len,
                           unsigned char key[16], unsigned char iv[16]);

#endif
