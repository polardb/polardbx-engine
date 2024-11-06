#include <stdlib.h>
#include <string.h>
#include "sm4.h"
#include "sm4_sha1.h"
#include "sm4_interface.h"
#include <assert.h>

extern int pkcs5_pbkdf2_hmac(const unsigned char *password,
                       size_t plen, const unsigned char *salt, size_t slen,
                       unsigned int iteration_count,
                       uint32_t key_length, unsigned char *output );


int GetKey(unsigned char* password,int passLen,unsigned char Key[16],unsigned char IV[16])
{
	int ret = STATUS_success;
	unsigned char salt[20]={0};
	size_t plen;
	unsigned char tmpBuf[64]={0};
	
	if (password==NULL || Key==NULL || IV==NULL)
	{
		return STATUS_parameter_error;
	}
	if (passLen<=0)
	{
		return STATUS_other_lenerror;
	}
	
	sha1_ret(password,passLen,salt);
	plen=passLen;
	ret=pkcs5_pbkdf2_hmac(password,plen,salt,20,1000,32,tmpBuf);
	if (ret!=0)
	{
		return STATUS_key_hash_error;
	}

	memcpy(Key,tmpBuf,16);
	memcpy(IV,tmpBuf+16,16);
	return ret;
}
int sym_encrypt_ctr_withKey(unsigned char *Key, int KeyLen,
                            const unsigned char IV[16],
                            const unsigned char *plain, int plainLen,
                            unsigned char *cipherbuf, int cipherbufLen,
                            int *cipherLen, unsigned int offset) {
  int i, j;
  int ret = STATUS_success;
  unsigned char ivlocal[16];
  sm4_context sm4_ctx;
  size_t conteroff;
  unsigned char ctrBuf[16] = {0};

  if (IV == NULL || plain == NULL || cipherbuf == NULL || cipherLen == NULL ||
      Key == NULL) {
    return STATUS_parameter_error;
  }

  if (KeyLen != 16) {
    return STATUS_other_lenerror;
  }

  if (plainLen == 0) {
    return STATUS_plain_lenerror;
  }
  if (cipherbufLen < plainLen) {
    return STATUS_buffer_tooshort;
  }

  memcpy(ivlocal, IV, 16);
  if (offset >= 16) {
    for (j = 0; j < (int)offset / 16; j++) {
      for (i = 16; i > 0; i--)
        if (++ivlocal[i - 1] != 0) break;
    }
  }

  sm4_init(&sm4_ctx);
  sm4_set_key(&sm4_ctx, (const unsigned char *)Key, KeyLen * 8);
  conteroff = offset % 16;
  if (conteroff != 0) {
    sm4_encrypt((const sm4_context *)&sm4_ctx, (const unsigned char *)ivlocal,
                ctrBuf);
    for (i = 16; i > 0; i--)
      if (++ivlocal[i - 1] != 0) break;
  }
  sm4_crypt_ctr(&sm4_ctx, plainLen, &conteroff, ivlocal, ctrBuf, plain,
                cipherbuf);
  *cipherLen = plainLen;
  sm4_free(&sm4_ctx);

  memset(ivlocal, 0, 16);
  memset(ctrBuf, 0, 16);
  offset = 0;
  return ret;
}

int sym_decrypt_ctr_withKey(unsigned char *Key, int KeyLen,
                            const unsigned char IV[16],
                            const unsigned char *cipher, int cipherLen,
                            unsigned char *plainbuf, int plainbufLen,
                            int *plainLen, unsigned int offset) {
  int i, j;
  int ret = STATUS_success;
  unsigned char ivlocal[16];
  sm4_context sm4_ctx;
  size_t conteroff;
  unsigned char ctrBuf[16] = {0};

  if (IV == NULL || plainbuf == NULL || cipher == NULL || plainLen == NULL ||
      Key == NULL) {
    return STATUS_parameter_error;
  }

  if (KeyLen != 16) {
    return STATUS_other_lenerror;
  }

  if (cipherLen == 0) {
    return STATUS_plain_lenerror;
  }
  if (plainbufLen < cipherLen) {
    return STATUS_buffer_tooshort;
  }

  memcpy(ivlocal, IV, 16);
  if (offset >= 16) {
    for (j = 0; j < (int)offset / 16; j++) {
      for (i = 16; i > 0; i--)
        if (++ivlocal[i - 1] != 0) break;
    }
  }

  sm4_init(&sm4_ctx);
  sm4_set_key(&sm4_ctx, (const unsigned char *)Key, KeyLen * 8);
  conteroff = offset % 16;
  if (conteroff != 0) {
    sm4_encrypt((const sm4_context *)&sm4_ctx, (const unsigned char *)ivlocal,
                ctrBuf);
    for (i = 16; i > 0; i--)
      if (++ivlocal[i - 1] != 0) break;
  }
  sm4_crypt_ctr(&sm4_ctx, cipherLen, &conteroff, ivlocal, ctrBuf, cipher,
                plainbuf);
  *plainLen = cipherLen;
  sm4_free(&sm4_ctx);

  memset(ivlocal, 0, 16);
  memset(ctrBuf, 0, 16);
  offset = 0;
  return ret;
}
