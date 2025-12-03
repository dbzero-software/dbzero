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
        // always the ABSOLUTE storage page number
        std::uint64_t m_end_storage_page_num;
        
        o_dp_changelog_header(StateNumType state_num, std::uint64_t end_storage_page_num)
            : m_state_num(state_num)
            , m_end_storage_page_num(end_storage_page_num)
        {
        }        
    };
DB0_PACKED_END
    
    extern template class o_change_log<db0::o_fixed_null>;
    extern template class o_change_log<o_dp_changelog_header>;

}
