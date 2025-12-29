// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "MetaIOStream.hpp"

namespace db0

{

    o_meta_item::o_meta_item(std::pair<std::uint64_t, std::uint64_t> stream_pos)
        : m_address(stream_pos.first)
        , m_stream_pos(stream_pos.second)
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

    MetaIOStream::MetaIOStream(CFile &m_file, const std::vector<BlockIOStream*> &managed_streams,
        std::uint64_t begin, std::uint32_t block_size, std::function<std::uint64_t()> tail_function,
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
        
        m_last_meta_log = &o_meta_log::__new(m_buffer.data(), state_num, meta_items);
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
                meta_items.emplace_back(m_managed_streams[i]->getStreamPos());
                m_last_stream_sizes[i] = stream_size;
            }
            appendMetaLog(state_num, meta_items);
        }
    }
    
    const o_meta_log *MetaIOStream::readMetaLog()
    {
        if (BlockIOStream::readChunk(m_buffer)) {
            m_last_meta_log = &o_meta_log::__const_ref(m_buffer.data());
            return m_last_meta_log;
        } else {
            return nullptr;
        }
    }
    
    const o_meta_log *MetaIOStream::tailMetaLog()
    {
        auto meta_log = readMetaLog();
        while (meta_log) {
            m_last_meta_log = meta_log;
            meta_log = readMetaLog();
        }
        return m_last_meta_log;
    }
    
    void MetaIOStream::setTailAll()
    {
        auto tail_log = tailMetaLog();
        if (tail_log) {
            auto it = tail_log->getMetaItems().begin();
            for (std::size_t i = 0; i < m_managed_streams.size(); ++i, ++it) {
                if (it == tail_log->getMetaItems().end()) {
                    THROWF(db0::IOException) << "MetaIOStream::setTailAll error: not enough meta items";
                }
                m_managed_streams[i]->setStreamPos(it->m_address, it->m_stream_pos);
                // exhaust the stream after setting the position
                while (m_managed_streams[i]->readChunk());
            }
        } else {
            // no tail item available, in such case simple exhaust all streams
            for (std::size_t i = 0; i < m_managed_streams.size(); ++i) {                
                // exhaust the stream
                while (m_managed_streams[i]->readChunk());
            }
        }
    }
    
    const o_meta_log *MetaIOStream::lowerBound(StateNumType state_num, std::vector<char> &buf) const
    {
        State state;
        saveState(state);
        const_cast<MetaIOStream*>(this)->setStreamPosHead();
        const o_meta_log *result = nullptr;
        try {
            while (true) {
                auto meta_log = const_cast<MetaIOStream*>(this)->readMetaLog();
                if (!meta_log || meta_log->m_state_num > state_num) {
                    break;
                }
                if (buf.size() < meta_log->sizeOf()) {
                    buf.resize(meta_log->sizeOf());
                }
                std::memcpy(buf.data(), meta_log, meta_log->sizeOf());
                result = &o_meta_log::__const_ref(buf.data());
                if (result->m_state_num == state_num) {
                    break;
                }
            }
        } catch (...) {
            const_cast<MetaIOStream*>(this)->restoreState(state);
            throw;
        }
        const_cast<MetaIOStream*>(this)->restoreState(state);
        return result;
    }
    
}