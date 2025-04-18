#include "MetaIOStream.hpp"

namespace db0

{

    o_meta_item::o_meta_item(std::pair<std::uint64_t, std::uint32_t> stream_pos)
        : m_address(stream_pos.first)
        , m_chunk_pos(stream_pos.second)
    {
    }

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
        , m_last_stream_sizes(managed_streams.size(), 0)
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
    
    bool MetaIOStream::checkAppend() const
    {
        std::size_t size_diff = 0;
        for (std::size_t i = 0; i < m_managed_streams.size(); ++i) {
            auto stream_size = m_managed_streams[i]->tell();
            size_diff += stream_size - m_last_stream_sizes[i];
            if (size_diff > m_step_max_size) {
                return true;
            }            
        }
        return false;        
    }
    
    void MetaIOStream::checkAndAppend(StateNumType state_num)
    {
        assert(m_access_type != AccessType::READ_ONLY);
        if (checkAppend()) {
            std::vector<o_meta_item> meta_items;
            for (std::size_t i = 0; i < m_managed_streams.size(); ++i) {
                auto stream_size = m_managed_streams[i]->tell();
                // store current position of the managed stream
                meta_items.emplace_back(m_managed_streams[i]->getCurrentPos());
                m_last_stream_sizes[i] = stream_size;
            }
            appendMetaLog(state_num, meta_items);            
        }
    }

    const o_meta_log *MetaIOStream::readMetaLog()
    {
        if (BlockIOStream::readChunk(m_buffer)) {
            return &o_meta_log::__const_ref(m_buffer.data());            
        } else {
            return nullptr;
        }
    }
    
}