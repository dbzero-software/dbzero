#include "ChangeLog.hpp"

namespace db0

{

    ChangeLogData::ChangeLogData(const std::vector<std::uint64_t> &change_log, bool rle_compress,
        bool add_duplicates, bool is_sorted)
        : m_change_log(change_log)
    {
        if (rle_compress) {
            initRLECompress(is_sorted, add_duplicates);
        }        
    }

    ChangeLogData::ChangeLogData(std::vector<std::uint64_t> &&change_log, bool rle_compress, bool add_duplicates, bool is_sorted)
        : m_change_log(std::move(change_log))
    {
        if (rle_compress) {
            initRLECompress(is_sorted, add_duplicates);
        }        
    }

    void ChangeLogData::initRLECompress(bool is_sorted, bool add_duplicates)
    {    
        if (!is_sorted) {
            std::sort(m_change_log.begin(), m_change_log.end());
        }
        for (auto value: m_change_log) {
            m_rle_builder.append(value, add_duplicates);
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
    
}