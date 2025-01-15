
#ifndef ssl_xdb_h
#define ssl_xdb_h

#include <openssl/ssl.h>

#ifdef  __cplusplus
extern "C" {
#endif

void ored_ssl_s3_flags(SSL *ssl, long flag);

void dh_set_p(DH *dh, BIGNUM *p);
void dh_set_g(DH *dh, BIGNUM *g);
BIGNUM *dh_get_p(DH *dh);
BIGNUM *dh_get_g(DH *dh);

#ifdef  __cplusplus
}
#endif

#endif

