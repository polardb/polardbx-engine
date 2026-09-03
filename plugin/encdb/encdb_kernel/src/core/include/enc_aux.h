/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 */
#pragma once

#include <stddef.h>
#include <stdint.h>
#include "bytes.h"
#include "encdb_cipher_types.h"
#include "encdb_error.h"
#include "encdb_sens_rule.h"
#include "encdb_sort.h"

namespace encdb {
EncdbStatus encdb_untrusted_encrypt(const bytes_t &input, uint8_t type,
                                    const std::string &username,
                                    bytes_t &output, UserAccess accs);
EncdbStatus encdb_untrusted_encrypt(const bytes_t &input, uint8_t type,
                                    const std::string &username,
                                    bytes_t &output, UserAccess accs,
                                    uint8_t algo);
EncdbStatus encdb_untrusted_decrypt(const bytes_t &input,
                                    const std::string &username,
                                    bytes_t &output, uint8_t &type);
uint32_t encdb_get_precise_cipher_size(const std::string &username,
                                       uint32_t in_size);
uint32_t encdb_get_precise_cipher_size(uint32_t in_size, uint8_t alg);
}  // namespace encdb
