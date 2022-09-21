//
// Created by zzy on 2022/9/5.
//

#pragma once

#include "../session/session_base.h"
#include "../utility/error.h"

#include "polarx_encoder.h"

namespace polarx_rpc {

err_t send_warnings(CsessionBase &session, CpolarxEncoder &encoder,
                    bool skip_single_error = false);

}
