/*****************************************************************************

Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

#ifndef TOKEN_BUCKET_H
#define TOKEN_BUCKET_H

#include <mutex>
#include <condition_variable>
#include <chrono>
#include <stdexcept>
#include <algorithm>

/** CPU cache line size */
#ifdef __powerpc__
constexpr size_t MY_CACHE_LINE_SIZE = 128;
#else
constexpr size_t MY_CACHE_LINE_SIZE = 64;
#endif /* __powerpc__ */

class TokenBucket {
public:
    /**
     * @param capacity the maximum capacity of the bucket (burst limit)
     * @param fill_rate the number of tokens generated per second (must be > 0)
     */
    TokenBucket(double capacity, double fill_rate)
        : capacity_(capacity)
        , fill_rate_(fill_rate)
        , tokens_(capacity)
        , last_refill_(std::chrono::steady_clock::now()) {
        if (capacity <= 0 || fill_rate <= 0) {
            throw std::invalid_argument("Capacity and fill_rate must be positive.");
        }
    }

    /**
     * Non-blocking attempt to acquire tokens
     * @param tokens the number of tokens to consume (default 1.0)
     * @return true for success, false for insufficient tokens
     */
    bool try_consume(double tokens = 1.0) {
        if (tokens <= 0) return true;

        std::unique_lock<std::mutex> lock(mutex_);
        refill();
        if (tokens_ >= tokens) {
            tokens_ -= tokens;
            return true;
        }
        return false;
    }

    /**
     * Blocking acquisition of tokens (using condition_variable to avoid busy waiting)
     * @param tokens the number of tokens to consume (default 1.0)
     */
    void consume(double tokens = 1.0) {
        if (tokens <= 0) return;

        std::unique_lock<std::mutex> lock(mutex_);
        while (true) {
            refill();
            if (tokens_ >= tokens) {
                tokens_ -= tokens;
                return;
            }

            // Calculate how many more tokens are needed
            double needed = tokens - tokens_;
            // Calculate the time needed to wait (seconds)
            double wait_seconds = needed / fill_rate_;

            // Use condition_variable to wait, avoiding busy waiting
            // Note: wait_for may be spuriously awakened, so it's placed in a while loop
            cv_.wait_for(lock, std::chrono::duration<double>(wait_seconds));
        }
    }

    /**
     * Dynamically modify the fill rate (thread-safe)
     * @param capacity the new maximum capacity
     * @param new_rate the new number of tokens per second (must be > 0)
     */
    void set_capacity_and_fill_rate(double capacity, double new_rate) {
        if (new_rate <= 0) {
            throw std::invalid_argument("Fill rate must be positive.");
        }

        std::unique_lock<std::mutex> lock(mutex_);
        refill(); // first update current token state
        fill_rate_ = new_rate;
        capacity_ = capacity;
    }

    /**
     * Get the current available token count (for monitoring or debugging)
     */
    double available_tokens() const {
        std::unique_lock<std::mutex> lock(mutex_);
        const_cast<TokenBucket*>(this)->refill(); // mutable or const_cast
        return tokens_;
    }

    double get_fill_rate() const {
        return fill_rate_;
    }

private:
    void refill() {
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration<double>(now - last_refill_).count();
        if (elapsed > 0) {
            tokens_ = std::min(capacity_, tokens_ + elapsed * fill_rate_);
            last_refill_ = now;
        }
    }

    double capacity_;
    double fill_rate_;
    alignas(MY_CACHE_LINE_SIZE) double tokens_;
    alignas(MY_CACHE_LINE_SIZE)
        std::chrono::time_point<std::chrono::steady_clock> last_refill_;

    mutable std::mutex mutex_;
    std::condition_variable cv_;
};

#endif //TOKEN_BUCKET_H
