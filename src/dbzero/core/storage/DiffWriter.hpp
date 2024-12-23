#pragma once

#include <cstdint>
#include <dbzero/core/serialization/Types.hpp>

namespace db0

{
    
    struct [[gnu::packed]] o_diff_header: public o_fixed<o_diff_header>
    {
        // the number of diff objects contained
        std::uint16_t m_size;
        // the continuation page number (0 if not needed)
        std::uint64_t m_next_page_num = 0;
    };

    class DiffWriter
    {
    public:
        DiffWriter(std::byte *buf, std::byte *end);
        
        void appendBytes(const std::byte *data, std::uint16_t size);        
        // Append (incomplete) stream of bytes & assign the continuation page number
        void appendContinuedBytes(const std::byte *data, std::uint16_t size);
        
    private:
        std::byte *m_buf;
        const std::byte *m_end;
        o_diff_header &m_header;
    };

}