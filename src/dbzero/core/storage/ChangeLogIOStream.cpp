#include "ChangeLogIOStream.hpp"
#include <algorithm>

namespace db0

{

    ChangeLogData::ChangeLogData(std::vector<std::uint64_t> &&change_log, bool rle_compress, bool add_duplicates, bool is_sorted)        
        : m_change_log(std::move(change_log))
    {
        if (rle_compress) {
            if (!is_sorted) {
                std::sort(m_change_log.begin(), m_change_log.end());
            }
            for (auto value: m_change_log) {
                m_rle_builder.append(value, add_duplicates);
            }
        }
    }
    
    o_change_log::o_change_log(const ChangeLogData &data)
        : m_rle_compressed(!data.m_rle_builder.empty())
    {
        if (m_rle_compressed) {
            arrangeMembers()
                (o_rle_sequence<std::uint64_t>::type(), data.m_rle_builder.getData());
        } else {
            arrangeMembers()
                (o_list<o_simple<std::uint64_t> >::type(), data.m_change_log);
        }
    }
    
    std::size_t o_change_log::measure(const ChangeLogData &data)
    {
        bool rle_compressed = !data.m_rle_builder.empty();
        if (rle_compressed) {
            return measureMembers()
                (o_rle_sequence<std::uint64_t>::type(), data.m_rle_builder.getData());
        } else {
            return measureMembers()
                (o_list<o_simple<std::uint64_t> >::type(), data.m_change_log);
        }
    }
    
    o_change_log::ConstIterator::ConstIterator(o_list<o_simple<std::uint64_t> >::const_iterator it)
        : m_rle(false)
        , m_list_it(it)
    {
    }

    o_change_log::ConstIterator::ConstIterator(o_rle_sequence<std::uint64_t>::ConstIterator it)
        : m_rle(true)
        , m_rle_it(it)
    {
    }

    o_change_log::ConstIterator &o_change_log::ConstIterator::operator++()
    {
        if (m_rle) {
            ++m_rle_it;
        } else {
            ++m_list_it;
        }
        return *this;
    }

    std::uint64_t o_change_log::ConstIterator::operator*() const
    {
        if (m_rle) {
            return *m_rle_it;
        } else {
            return *m_list_it;
        }
    }

    bool o_change_log::ConstIterator::operator!=(const ConstIterator &other) const
    {
        if (m_rle != other.m_rle) {
            return true;
        }
        if (m_rle) {
            return m_rle_it != other.m_rle_it;
        } else {
            return m_list_it != other.m_list_it;
        }
    }
    
    o_change_log::ConstIterator o_change_log::begin() const
    {
        if (m_rle_compressed) {
            return ConstIterator(rle_sequence().begin());
        } else {
            return ConstIterator(changle_log().begin());
        }
    }

    o_change_log::ConstIterator o_change_log::end() const
    {
        if (m_rle_compressed) {
            return ConstIterator(rle_sequence().end());
        } else {
            return ConstIterator(changle_log().end());
        }
    }
    
    bool o_change_log::isRLECompressed() const {
        return m_rle_compressed;
    }

    ChangeLogIOStream::ChangeLogIOStream(CFile &m_file, std::uint64_t begin, std::uint32_t block_size,
        std::function<std::uint64_t()> tail_function, AccessType access_type)
        // enable checksums
        : BlockIOStream(m_file, begin, block_size, tail_function, access_type, true)
    {
    }

    ChangeLogIOStream::ChangeLogIOStream(BlockIOStream &&io_stream)
        : BlockIOStream(std::move(io_stream))
    {
    }
    
    const o_change_log &ChangeLogIOStream::appendChangeLog(ChangeLogData &&data)
    {        
        auto size_of = o_change_log::measure(data);
        if (m_buffer.size() < size_of) {
            m_buffer.resize(size_of);
        }

        o_change_log::__new(m_buffer.data(), data);
        // append change log as a separate chunk
        BlockIOStream::addChunk(size_of);
        BlockIOStream::appendToChunk(m_buffer.data(), size_of);
        m_last_change_log_ptr = &o_change_log::__const_ref(m_buffer.data());
        assert(m_last_change_log_ptr->sizeOf() == size_of);

        return *m_last_change_log_ptr;
    }
    
    const o_change_log *ChangeLogIOStream::readChangeLogChunk()
    {
        if (BlockIOStream::readChunk(m_buffer)) {
            m_last_change_log_ptr = &o_change_log::__const_ref(m_buffer.data());
            return m_last_change_log_ptr;
        } else {
            return nullptr;
        }
    }

    const o_change_log *ChangeLogIOStream::getLastChangeLogChunk() const {
        return m_last_change_log_ptr;
    }

}