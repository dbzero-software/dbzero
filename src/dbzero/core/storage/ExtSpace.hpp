#pragma once

#include "BaseStorage.hpp"
#include "ChangeLogIOStream.hpp"
#include "REL_Index.hpp"
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/dram/DRAMSpace.hpp>

namespace db0

{

DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_ext_space: public o_fixed<o_ext_space>
    {   
        // the primary (mandatory) and secondary (optional) REL_Index addresses
        std::array<Address, 2> m_rel_index_addr;
        // reserved for future use
        std::array<std::uint64_t, 4> m_reserved;
        
        o_ext_space();
    };
DB0_PACKED_END
    
    // The ExtSpace manages extension indexes (e.g. REL_Index)
    class ExtSpace
    {
    public:
        using DP_ChangeLogT = BaseStorage::DP_ChangeLogT;
        using DP_ChangeLogStreamT = db0::ChangeLogIOStream<DP_ChangeLogT>;
        struct tag_create {};
        
        // NOTE: dram pair may be nullptr (for a null ExtSpace)
        ExtSpace(tag_create, DRAM_Pair, std::uint32_t step_size);
        ExtSpace(DRAM_Pair, AccessType, std::uint32_t step_size);
        ~ExtSpace();
        
        // get the primary REL_Index
        inline REL_Index &getREL_Index() {
            return m_rel_index;
        }
        
        void refresh();
        void commit();
        
    private:
        std::shared_ptr<DRAM_Prefix> m_dram_prefix;
        std::shared_ptr<DRAM_Allocator> m_dram_allocator;
        Memspace m_dram_space;
        const AccessType m_access_type;
        // the root object (created at address 0)
        db0::v_object<o_ext_space> m_ext_space_root;
        REL_Index m_rel_index;

        db0::v_object<o_ext_space> tryOpenRoot() const;
        REL_Index tryOpenPrimaryREL_Index(std::uint32_t step_size) const;
    };
    
}