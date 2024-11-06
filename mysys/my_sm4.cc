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

#include "my_sm4.h"
#include <memory.h>
#include "sm4.h"
#include "sm4_interface.h"

/**
  Transforms an arbitrary long key into 16 bytes key.
  @param [in]  k     Original key content
  @param [in]  k_len Original key length
  @param [out] rkey  16 bytes length SM4 key
*/
static void to_sm4_key(const unsigned char *k, uint32 k_len,
                       unsigned char *rkey) {
  unsigned char *rkey_end;      /* Real key boundary */
  unsigned char *ptr;           /* Start of the real key*/
  const unsigned char *sptr;    /* Start of the working key */
  const unsigned char *key_end; /* Working key boundary*/

  key_end = k + k_len;
  rkey_end = rkey + SM4_KEY_LENGTH;
  memset(rkey, 0, SM4_KEY_LENGTH); /* Set initial key */
  for (ptr = rkey, sptr = k; sptr < key_end; ptr++, sptr++) {
    if (ptr == rkey_end) /*  Just loop over tmp_key until we used all key */
      ptr = rkey;
    *ptr ^= *sptr;
  }
}

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
                   const unsigned char *iv) {
  if (mode == SM4_CTR) {
    int encryted_len;
    unsigned char k[SM4_KEY_LENGTH];
    to_sm4_key(key, key_length, k);
    int ret = sym_encrypt_ctr_withKey(k, SM4_KEY_LENGTH, iv, source,
                                      (int)source_length, dest,
                                      (int)source_length, &encryted_len, 0);
    if (ret == 0 && encryted_len == (int)source_length) return encryted_len;
  } else if (mode == SM4_ECB) {
    sm4_context sm4_ctx;
    if (sm4_set_key(&sm4_ctx, key, (size_t)key_length)) return -1;
    sm4_crypt_ecb(&sm4_ctx, source, dest, (size_t)source_length, 1);
    return (int)source_length;
  }

  return -1;
}

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
                   const unsigned char *iv) {
  if (mode == SM4_CTR) {
    int decryted_len;
    unsigned char k[SM4_KEY_LENGTH];
    to_sm4_key(key, key_length, k);
    int ret = sym_decrypt_ctr_withKey(k, SM4_KEY_LENGTH, iv, source,
                                      (int)source_length, dest,
                                      (int)source_length, &decryted_len, 0);
    if (ret == 0 && decryted_len == (int)source_length) return decryted_len;
  } else if (mode == SM4_ECB) {
    sm4_context sm4_ctx;
    if (sm4_set_key(&sm4_ctx, key, (size_t)key_length)) return -1;
    sm4_crypt_ecb(&sm4_ctx, source, dest, (size_t)source_length, 0);
    return (int)source_length;
  }

  return -1;
}

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
                           unsigned char key[16], unsigned char iv[16]) {
  return GetKey(passwd, (int)passwd_len, key, iv);
}
