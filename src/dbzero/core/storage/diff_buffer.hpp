// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/serialization/Base.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{

DB0_PACKED_BEGIN
    class DB0_PACKED_ATTR o_diff_buffer: public o_base<o_diff_buffer, 0, false>
    {
    protected:
        using super_t = o_base<o_diff_buffer, 0, false>;
        friend super_t;

        // @param dp_data the data page buffer
        // @param result vector to hold size of diff / size of identical areas interleaved (totalling to size)
        o_diff_buffer(const std::byte *dp_data, const std::vector<std::uint16_t> &diff_buf);

    public:
        // size of the entire diff buffer
        std::uint16_t m_size;

        static std::size_t measure(const std::byte *dp_data, const std::vector<std::uint16_t> &diff_buf);

        template <typename buf_t> static std::size_t safeSizeOf(buf_t at)
        {
            auto size = o_diff_buffer::__const_ref(at).m_size;
            at += size;
            return size;
        }
        
        // Apply diffs on to a specific data-page
        // @param dp_result / dp_end - the buffer to hold the result data-page affter diff application
        void apply(std::byte *dp_result, const std::byte *dp_end) const;
        
        static std::size_t sizeOfHeader() {
            return sizeof(o_diff_buffer);
        }
    };    
DB0_PACKED_END
    
}