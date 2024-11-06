#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include"sm4_sha1.h"

#define MD_MAX_SIZE         64  /* longest known is SHA512 */

#define MBEDTLS_ERR_MD_FEATURE_UNAVAILABLE                -0x5080  /**< The selected feature is not available. */
#define ERR_MD_BAD_INPUT_DATA                     -0x5100  /**< Bad input parameters to function. */
#define ERR_MD_ALLOC_FAILED                       -0x5180  /**< Failed to allocate memory. */
#define ERR_MD_FILE_IO_ERROR                      -0x5200  /**< Opening or reading of file failed. */

typedef struct md_context_t
{
    /** The digest-specific context. */
    // void *md_ctx;
    sha1_context *md_ctx;

    /** The HMAC part of the context. */
    void *hmac_ctx;
} md_context_t;


int md_hmac_starts( md_context_t *ctx, const unsigned char *key, size_t keylen )
{
    int ret;
    unsigned char sum[MD_MAX_SIZE];
    unsigned char *ipad, *opad;
    size_t i;

    if( ctx == NULL  || ctx->hmac_ctx == NULL )
        return( ERR_MD_BAD_INPUT_DATA );

    if( keylen > 64 )
    {
        if( ( ret = sha1_starts_ret( ctx->md_ctx ) ) != 0 )
            goto cleanup;
        if( ( ret = sha1_update_ret( ctx->md_ctx, key, keylen ) ) != 0 )
            goto cleanup;
        if( ( ret = sha1_finish_ret( ctx->md_ctx, sum ) ) != 0 )
            goto cleanup;

        keylen = 20;
        key = sum;
    }

    ipad = (unsigned char *) ctx->hmac_ctx;
    opad = (unsigned char *) ctx->hmac_ctx + 64;

    memset( ipad, 0x36, 64 );
    memset( opad, 0x5C,64 );

    for( i = 0; i < keylen; i++ )
    {
        ipad[i] = (unsigned char)( ipad[i] ^ key[i] );
        opad[i] = (unsigned char)( opad[i] ^ key[i] );
    }

    if( ( ret = sha1_starts_ret( ctx->md_ctx ) ) != 0 )
        goto cleanup;
    if( ( ret = sha1_update_ret( ctx->md_ctx, ipad,64 ) ) != 0 )
        goto cleanup;

cleanup:
    memset( sum,0, sizeof( sum ) );

    return( ret );
}

int md_hmac_update( md_context_t *ctx, const unsigned char *input, size_t ilen )
{
    if( ctx == NULL || ctx->hmac_ctx == NULL )
        return( ERR_MD_BAD_INPUT_DATA );

    return( sha1_update_ret( ctx->md_ctx, input, ilen ) );
}

int md_hmac_finish( md_context_t *ctx, unsigned char *output )
{
    int ret;
    unsigned char tmp[MD_MAX_SIZE];
    unsigned char *opad;

    if( ctx == NULL || ctx->hmac_ctx == NULL )
        return( ERR_MD_BAD_INPUT_DATA );

    opad = (unsigned char *) ctx->hmac_ctx + 64;

    if( ( ret =sha1_finish_ret( ctx->md_ctx, tmp ) ) != 0 )
        return( ret );
    
    if( ( ret = sha1_starts_ret( ctx->md_ctx ) ) != 0 )
        return( ret );
    if( ( ret = sha1_update_ret( ctx->md_ctx, opad,64 ) ) != 0 )
        return( ret );
    if( ( ret = sha1_update_ret( ctx->md_ctx, tmp, 20) ) != 0 )
        return( ret );
    return( sha1_finish_ret(ctx->md_ctx, output ) );
}


int pkcs5_pbkdf2_hmac(const unsigned char *password,
                       size_t plen, const unsigned char *salt, size_t slen,
                       unsigned int iteration_count,
                       uint32_t key_length, unsigned char *output )
{
    int ret=0, j;
    unsigned int i;
    md_context_t ctx;
    unsigned char md1[MD_MAX_SIZE];
    unsigned char work[MD_MAX_SIZE];
    unsigned char md_size = 20;
    size_t use_len;
    unsigned char *out_p = output;
    unsigned char counter[4];

    memset( counter, 0, 4 );
    counter[3] = 1;

    memset(&ctx,0,sizeof(md_context_t));
    ctx.md_ctx=(sha1_context*)calloc(1,sizeof(sha1_context));
    if (ctx.md_ctx==NULL)
    {
        return ERR_MD_ALLOC_FAILED;
    }
    ctx.hmac_ctx=calloc(1,128);
    if (ctx.hmac_ctx==NULL)
    {
        free(ctx.md_ctx);
        return ERR_MD_ALLOC_FAILED;
    }
    
    
    while( key_length )
    {
        if( ( ret = md_hmac_starts( &ctx, password, plen ) ) != 0 )
            goto cleanup;

        if( ( ret = md_hmac_update( &ctx, salt, slen ) ) != 0 )
             goto cleanup;

        if( ( ret = md_hmac_update( &ctx, counter, 4 ) ) != 0 )
            goto cleanup;

        if( ( ret = md_hmac_finish( &ctx, work ) ) != 0 )
            goto cleanup;

        memcpy( md1, work, md_size );

        for( i = 1; i < iteration_count; i++ )
        {
            // U2 ends up in md1
            //
            if( ( ret = md_hmac_starts( &ctx, password, plen ) ) != 0 )
                goto cleanup;

            if( ( ret = md_hmac_update( &ctx, md1, md_size ) ) != 0 )
                goto cleanup;

            if( ( ret = md_hmac_finish( &ctx, md1 ) ) != 0 )
                goto cleanup;

            // U1 xor U2
            //
            for( j = 0; j < md_size; j++ )
                work[j] ^= md1[j];
        }

        use_len = ( key_length < md_size ) ? key_length : md_size;
        memcpy( out_p, work, use_len );

        key_length -= (uint32_t) use_len;
        out_p += use_len;

        for( i = 4; i > 0; i-- )
            if( ++counter[i - 1] != 0 )
                break;
    }

cleanup:
    free(ctx.hmac_ctx);
    free(ctx.md_ctx);
    return ret;
}