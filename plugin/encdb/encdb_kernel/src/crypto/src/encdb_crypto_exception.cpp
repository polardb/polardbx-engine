/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: definition exceptions for crypto module
 */
#include "encdb_crypto_exception.h"
#include <string>
#include "encdb_common_exception.h"

namespace encdb {

CryptoException::CryptoException(EncdbStatus code, std::string msg)
    : EncdbException(code, msg) {}

NoSuchAlgorithmException::NoSuchAlgorithmException(std::string msg)
    : CryptoException(ERROR_NO_SUCH_ALGORITHM, msg) {}

NoSuchCCFlagsException::NoSuchCCFlagsException(std::string msg)
    : CryptoException(ERROR_NO_SUCH_CC_FLAGS, msg) {}

NoSuchCipherSuiteException::NoSuchCipherSuiteException(std::string msg)
    : CryptoException(ERROR_NO_SUCH_CIPHER_SUITE, msg) {}

NoSuchCCPolicyException::NoSuchCCPolicyException(std::string msg)
    : CryptoException(ERROR_NO_SUCH_CC_POLICY, msg) {}

InvalidAlgorithmParameterException::InvalidAlgorithmParameterException(
    std::string msg)
    : CryptoException(ERROR_INVALID_PARAMETER, msg) {}

InvalidKeyException::InvalidKeyException(std::string msg)
    : CryptoException(ERROR_INVALID_KEY, msg) {}

MacMismatchException::MacMismatchException(std::string msg)
    : CryptoException(ERROR_MAC_MISMATCH, msg) {}

EncdbOldMekMissException::EncdbOldMekMissException(std::string msg)
    : CryptoException(ENCDB_EXCEPTION_OLD_MEK_MISS, msg) {}

BadPaddingException::BadPaddingException(std::string msg)
    : CryptoException(ERROR_BAD_PADDING, msg) {}

BadCipherException::BadCipherException(std::string msg)
    : CryptoException(ERROR_BAD_CIPHER, msg) {}

}  // namespace encdb
