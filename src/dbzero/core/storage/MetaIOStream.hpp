// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "BlockIOStream.hpp"
#include <vector>
#include <dbzero/core/serialization/list.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{

DB0_PACKED_BEGIN
    // Single managed-stream associated item
    struct DB0_PACKED_ATTR o_meta_item: public o_fixed<o_meta_item>
    {
        // the absolute file position in the managed stream
        std::uint64_t m_address = 0;
        // the position in the managed stream (relative)
        std::uint64_t m_stream_pos = 0;
        
        o_meta_item(std::pair<std::uint64_t, std::uint64_t> stream_pos);
    };
DB0_PACKED_END    
    
DB0_PACKED_BEGIN
    // The single log item, possibly associated with multiple managed streams
    class DB0_PACKED_ATTR o_meta_log: public o_base<o_meta_log, 0, true>
    {
    protected:
        friend class o_base<o_meta_log, 0, true>;

        o_meta_log(StateNumType state_num, const std::vector<o_meta_item> &);

    public:
        // the log's corresponding state number
        StateNumType m_state_num = 0;

        const o_list<o_meta_item> &getMetaItems() const;

        static std::size_t measure(StateNumType, const std::vector<o_meta_item> &);

        template <typename T> static std::size_t safeSizeOf(T buf)
        {
            return sizeOfMembers(buf)
                (o_list<o_meta_item>::type());
        }
    };
DB0_PACKED_END
    
    // The MetaIOStream is used to annotate data (i.e. state numbers and the corresponding file positions)
    // in the underlying managed ChangeLogIOStream-s
    // the purpose is to speed-up retrieval and initialization of the streams for append
    class MetaIOStream: public BlockIOStream
    {
    public:
        // checksums disabled in this type of stream
        static constexpr bool ENABLE_CHECKSUMS = false;
        
        // @param step_size the cummulative change in the managed streams' size to be reflected in the meta stream
        MetaIOStream(CFile &m_file, const std::vector<BlockIOStream*> &managed_streams, std::uint64_t begin,
            std::uint32_t block_size, std::function<std::uint64_t()> tail_function = {}, AccessType = AccessType::READ_WRITE, 
            bool maintain_checksums = false, std::size_t step_size = 16 << 20);
        
        // Check the underlying managed streams and append the meta log if needed (i.e. if step size is reached)
        void checkAndAppend(StateNumType state_num);
        
        /**
         * Read a single meta-log from the stream.
         * The operation overwrites result of the previous read (unless nullptr is returned)        
         * @return the meta-log or nullptr if end of the stream reached
        */
        const o_meta_log *readMetaLog();
        
        // Read until the end of the stream and retrieve the last meta-log (if available)
        const o_meta_log *tailMetaLog();
        
        // Set this add all underlying streams to end/tail positions
        void setTailAll();
        
        // Try locating an entry either equal or lower than the given state number
        // @param state_num the state number to be located
        // @param buf a meta-log persistency buffer
        // @return the meta-log or nullptr if not found
        const o_meta_log *lowerBound(StateNumType, std::vector<char> &buf) const;
        
        std::uint64_t getStepSize() const {
            return m_step_max_size;
        }
        
    private:
        const std::vector<BlockIOStream*> m_managed_streams;
        // stream sizes at the last meta log item (the last checkpoint)
        std::vector<std::size_t> m_last_stream_sizes;
        const std::size_t m_step_max_size;        
        // a temporary buffer for meta log items
        std::vector<char> m_buffer;
        const o_meta_log *m_last_meta_log = nullptr;
        
        // check if more than m_step_max_size bytes were appended to the managed streams
        bool checkAppend() const;
        void appendMetaLog(StateNumType state_num, const std::vector<o_meta_item> &meta_items);
    };
    
}
