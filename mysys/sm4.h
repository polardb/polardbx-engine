#ifndef SM4_H
#define SM4_H

#include <stddef.h>
#include <stdint.h>
#define SM4_KEY_LENGTH           16
#define SM4_BLOCK_SIZE           16
#define SM4_IV_LENGTH            SM4_BLOCK_SIZE
#define SM4_NUM_ROUNDS           32

#define SM4_ENCRYPT              1
#define SM4_DECRYPT              0

#ifdef __cplusplus
extern "C" {
#endif

typedef struct sm4_context
{
    uint32_t key[SM4_NUM_ROUNDS];   /*SOFT ALGORITHM*/
}sm4_context;

/**
 * \brief          Initialize SM4 context
 *
 * \param ctx      SM4 context to be initialized
 */
void sm4_init( sm4_context *ctx );

/**
 * \brief          Clear SM4 context
 *
 * \param ctx      SM4 context to be cleared
 */
void sm4_free( sm4_context *ctx );

/**
 * \brief          SM4 key schedule
 *
 * \param ctx      SM4 context to be initialized
 * \param userKey  16-byte secret key
 * \param length   secret key length, it's 16
 *
 */
int sm4_set_key(sm4_context *ctx, const unsigned char *userKey, size_t length);
void sm4_encrypt(const sm4_context *ctx, const unsigned char *in, unsigned char *out);
int sm4_crypt_ecb(const sm4_context *ctx,const unsigned char *in, unsigned char *out, size_t length, const int enc);


int sm4_crypt_ctr( sm4_context *ctx,
                       size_t length,
                       size_t *nc_off,
                       unsigned char nonce_counter[16],
                       unsigned char stream_block[16],
                       const unsigned char *input,
                       unsigned char *output );

#ifdef __cplusplus
}
#endif

#endif