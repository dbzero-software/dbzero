#pragma once

#include "BlockIOStream.hpp"
#include <vector>
#include <dbzero/core/serialization/list.hpp>

namespace db0

{

    // Single managed-stream associated item
    struct [[gnu::packed]] o_meta_item: public o_fixed<o_meta_item>
    {
        // the absolute file position in the managed stream
        std::uint64_t m_address = 0;
        // the total stream size at this position (tell)
        std::uint64_t m_size = 0;
    };
    
    // The single log item, possibly associated with multiple managed streams
    class [[gnu::packed]] o_meta_log: public o_base<o_meta_log, 0, false>
    {
    protected:
        friend class o_base<o_meta_log, 0, false>;

        o_meta_log(StateNumType state_num, const std::vector<o_meta_item> &);

    public:
        // the log's corresponding state number
        StateNumType m_state_num = 0;

        const o_list<o_meta_item> &getMetaItems() const;

        static std::size_t measure(StateNumType, const std::vector<o_meta_item> &);

        template <typename T> static std::size_t safeSizeOf(T buf)
        {
            auto _buf = buf;
            buf += o_simple<StateNumType>::__const_ref(buf).sizeOf();
            buf += o_list<o_meta_item>::safeSizeOf(buf);
            return buf - _buf;
        }
    };

    // The MetaIOStream is used to annotate data (i.e. state numbers and the corresponding file positions)
    // in the underlying managed ChangeLogIOStream-s
    // the purpose is to speed-up retrieval and initialization of the streams for append
    class MetaIOStream: public BlockIOStream
    {
    public:
        // @param step_size the cummulative change in the managed streams' size to be reflected in the meta stream
        MetaIOStream(CFile &m_file, std::uint64_t begin, std::uint32_t block_size,
            std::function<std::uint64_t()> tail_function, const std::vector<const BlockIOStream*> &managed_streams,
            AccessType = AccessType::READ_WRITE, bool maintain_checksums = false, 
            std::size_t step_size = 16 << 20);
        
        // Check the underlying managed streams and append the meta log if needed (step size is reached)
        void checkAndAppend(StateNumType state_num);
        
    private:
        const std::vector<const BlockIOStream*> m_managed_streams;
        const std::size_t m_step_max_size;
        std::size_t m_current_step_size = 0;
        // a temporary buffer for meta log items
        std::vector<std::byte> m_buffer;
        
        void appendMetaLog(StateNumType state_num, const std::vector<o_meta_item> &meta_items);
    };

}
