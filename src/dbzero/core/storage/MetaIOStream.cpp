#include "MetaIOStream.hpp"

namespace db0

{

    o_meta_log::o_meta_log(StateNumType state_num, const std::vector<o_meta_item> &meta_items) 
        : m_state_num(state_num)
    {
        arrangeMembers()
            (o_list<o_meta_item>::type(), meta_items);
    }

    const o_list<o_meta_item> &o_meta_log::getMetaItems() const {
        return this->getDynFirst(o_list<o_meta_item>::type());
    }   
    
    std::size_t o_meta_log::measure(StateNumType, const std::vector<o_meta_item> &meta_items)
    {
        return measureMembers()
            (o_list<o_meta_item>::type(), meta_items);
    }

    MetaIOStream::MetaIOStream(CFile &m_file, std::uint64_t begin, std::uint32_t block_size,
        std::function<std::uint64_t()> tail_function, const std::vector<const BlockIOStream*> &managed_streams,
        AccessType access_type, bool maintain_checksums, std::size_t step_size)
        : BlockIOStream(m_file, begin, block_size, tail_function, access_type, maintain_checksums)
        , m_managed_streams(managed_streams)
        , m_step_max_size(step_size)
    {
    }    
    
    void MetaIOStream::appendMetaLog(StateNumType state_num, const std::vector<o_meta_item> &meta_items)
    {
        auto size_of = o_meta_log::measure(state_num, meta_items);
        if (m_buffer.size() < size_of) {
            m_buffer.resize(size_of);
        }

        o_meta_log::__new(m_buffer.data(), state_num, meta_items);
        // append meta-log item as a separate chunk
        BlockIOStream::addChunk(size_of);
        BlockIOStream::appendToChunk(m_buffer.data(), size_of);
    }

}