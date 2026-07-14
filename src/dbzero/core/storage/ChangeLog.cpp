// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ChangeLog.hpp"
#include "ChangeLogTypes.hpp"

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
    
    template <typename BaseT>
    o_change_log<BaseT>::ConstIterator::ConstIterator(o_list<o_simple<std::uint64_t> >::const_iterator it)
        : m_rle(false)
        , m_list_it(it)
    {
    }

    template <typename BaseT>
    o_change_log<BaseT>::ConstIterator::ConstIterator(o_rle_sequence<std::uint64_t>::ConstIterator it)
        : m_rle(true)
        , m_rle_it(it)
    {
    }

    template <typename BaseT>
    typename o_change_log<BaseT>::ConstIterator &o_change_log<BaseT>::ConstIterator::operator++()
    {
        if (m_rle) {
            ++m_rle_it;
        } else {
            ++m_list_it;
        }
        return *this;
    }

    template <typename BaseT>
    std::uint64_t o_change_log<BaseT>::ConstIterator::operator*() const
    {
        if (m_rle) {
            return *m_rle_it;
        } else {
            return *m_list_it;
        }
    }

    template <typename BaseT>
    bool o_change_log<BaseT>::ConstIterator::operator!=(const ConstIterator &other) const
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
    
    template <typename BaseT>
    typename o_change_log<BaseT>::ConstIterator o_change_log<BaseT>::begin() const
    {
        if (this->isRLECompressed()) {
            return ConstIterator(rle_sequence().begin());
        } else {
            return ConstIterator(changle_log().begin());
        }
    }

    template <typename BaseT>
    typename o_change_log<BaseT>::ConstIterator o_change_log<BaseT>::end() const
    {
        if (this->isRLECompressed()) {
            return ConstIterator(rle_sequence().end());
        } else {
            return ConstIterator(changle_log().end());
        }
    }
    
    template <typename BaseT>
    bool o_change_log<BaseT>::isRLECompressed() const {
        return rleCompressed().value();
    }
    
    template class o_change_log<>;
    template class o_change_log<o_dram_changelog_header>;
    template class o_change_log<o_dp_changelog_header>;
    
}