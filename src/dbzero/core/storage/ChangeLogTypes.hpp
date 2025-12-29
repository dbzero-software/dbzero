// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "ChangeLog.hpp"

namespace db0

{
    
DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_dp_changelog_header: o_fixed<o_dp_changelog_header>
    {        
        // state number this change log corresponds to
        StateNumType m_state_num;
        // sentinel storage page number for this transaction (see Page_IO::getEndPageNum())
        // NOTE: this value might be relative if the mapping is active
        std::uint64_t m_end_storage_page_num;
        // reserved for future use
        std::array<std::uint64_t, 2> m_reserved = { 0, 0 };
        
        o_dp_changelog_header(StateNumType state_num, std::uint64_t end_storage_page_num)
            : m_state_num(state_num)
            , m_end_storage_page_num(end_storage_page_num)
        {
        }        
    };
DB0_PACKED_END
    
DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_dram_changelog_header: o_fixed<o_dram_changelog_header>
    {
        // state number this change log corresponds to
        StateNumType m_state_num;
        // reserved for future use
        std::array<std::uint64_t, 2> m_reserved = { 0, 0 };
        
        o_dram_changelog_header(StateNumType state_num)
            : m_state_num(state_num)            
        {
        }        
    };
DB0_PACKED_END
    
    extern template class o_change_log<>;
    extern template class o_change_log<o_dram_changelog_header>;
    extern template class o_change_log<o_dp_changelog_header>;

}
