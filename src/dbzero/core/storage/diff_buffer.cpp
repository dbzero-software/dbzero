// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "diff_buffer.hpp"
#include <dbzero/core/serialization/packed_int.hpp>
#include <cstring>
#include <cassert>
#include <limits>

namespace db0

{   
    
    o_diff_buffer::o_diff_buffer(const std::byte *dp_data, const std::vector<std::uint16_t> &diff_buf)
    {
        std::byte *at = (std::byte*)this + sizeof(o_diff_buffer);
        auto it = diff_buf.begin();
        while (it != diff_buf.end()) {
            o_packed_int<std::uint16_t>::write(at, *it);
            if (*it > 0) {
                std::memcpy(at, dp_data, *it);
                at += *it;
                dp_data += *it;
            }
            ++it;
            if (it == diff_buf.end()) {
                break;
            }
            // identical area
            o_packed_int<std::uint16_t>::write(at, *it);
            dp_data += *it;
            ++it;
        }
        assert(at <= (std::byte*)this + std::numeric_limits<std::uint16_t>::max());
        m_size = at - (std::byte*)this;
    }
    
    std::size_t o_diff_buffer::measure(const std::byte *, const std::vector<std::uint16_t> &diff_buf)
    {
        std::size_t result = sizeof(o_diff_buffer);
        auto it = diff_buf.begin();
        while (it != diff_buf.end()) {
            result += o_packed_int<std::uint16_t>::measure(*it);            
            result += *it;            
            ++it;
            if (it == diff_buf.end()) {
                break;
            }
            result += o_packed_int<std::uint16_t>::measure(*it);
            ++it;
        }
        return result;
    }
    
    void o_diff_buffer::apply(std::byte *dp_result, const std::byte *dp_end) const
    {
        // apply diffs next
        const std::byte *at = (std::byte*)this + sizeof(o_diff_buffer);
        auto end = (std::byte*)this + m_size;
        while (at < end) {
            auto diff_size = o_packed_int<std::uint16_t>::read(at, end);
            if (diff_size > 0) {
                assert(dp_result + diff_size <= dp_end);
                // this check prevents processing of corrupt diff data
                if (dp_result + diff_size > dp_end) {
                    THROWF(db0::IOException) << "o_diff_buffer::apply: corrupt diff data";
                }
                std::memcpy(dp_result, at, diff_size);
                dp_result += diff_size;
                at += diff_size;
            }
            if (at < end) {
                auto identical_size = o_packed_int<std::uint16_t>::read(at, end);
                dp_result += identical_size;
                if (dp_result > dp_end) {
                    THROWF(db0::IOException) << "o_diff_buffer::apply: corrupt diff data";
                }
                // zero-fill when the indicator is present (special 0,0 indicator)
                if (!diff_size && !identical_size) {
                    // make sure the indicator is only present at the beginning
                    assert(at <= (std::byte*)this + sizeof(o_diff_buffer) + sizeof(std::uint16_t) * 3);
                    std::memset(dp_result, 0, dp_end - dp_result);
                }            
            }
        }
    }
    
}