#pragma once

#include <dbzero/core/serialization/Types.hpp>
#include "BaseStorage.hpp"
#include "ChangeLogIOStream.hpp"
#include "REL_Index.hpp"

namespace db0

{

DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_ext_space: public o_fixed<o_ext_space>
    {   
        // the primary (mandatory) and secondary (optional) REL_Index addresses
        std::array<std::uint64_t, 2> m_rel_index_addr = {0, 0};
        // reserved for future use
        std::array<std::uint64_t, 4> m_reserved;
    };
DB0_PACKED_END
    
    // The ExtSpace manages extension indexes (e.g. REL_Index)
    class ExtSpace
    {
    public:
        using DP_ChangeLogT = BaseStorage::DP_ChangeLogT;
        using DP_ChangeLogStreamT = db0::ChangeLogIOStream<DP_ChangeLogT>;
        
        // NOTE: dram pair may be nullptr (for a null ExtSpace)
        ExtSpace(DRAM_Pair, AccessType);
        ~ExtSpace();
        
        // get the primary REL_Index
        inline REL_Index &getREL_Index() {
            return m_rel_index;
        }
        
        void refresh();
        void commit();
        
    private:
        REL_Index m_rel_index;
    };
    
}