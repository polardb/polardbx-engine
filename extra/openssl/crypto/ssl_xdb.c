
#include <openssl/ssl_xdb.h>
#include "ssl/ssl_local.h"
#include "crypto/dh/dh_local.h"

void ored_ssl_s3_flags(SSL *ssl, long flag) {
    if (ssl->s3) {
        ssl->s3->flags |= flag;
    }
}

void dh_set_p(DH *dh, BIGNUM *p) { dh->p = p; }
void dh_set_g(DH *dh, BIGNUM *g) { dh->g = g; }

BIGNUM *dh_get_p(DH *dh) { return dh->p; }
BIGNUM *dh_get_g(DH *dh) { return dh->g; }

