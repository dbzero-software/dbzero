#pragma once

#include "BaseStorage.hpp"
#include "ChangeLogIOStream.hpp"
#include "REL_Index.hpp"
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/dram/DRAMSpace.hpp>

namespace db0

{

    // NOTE: o_ext_space must occupy the entire DP (due to DRAM Allocator requirements)
DB0_PACKED_BEGIN
    class DB0_PACKED_ATTR o_ext_space: public o_base<o_ext_space, 0, false>
    {   
        using super_t = o_base<o_ext_space, 0, false>;
        friend super_t;

        o_ext_space(std::uint32_t page_size);
        
    public:
        const std::uint32_t m_page_size;
        // the primary (mandatory) and secondary (optional) REL_Index addresses
        std::array<std::uint64_t, 2> m_rel_index_addr = { 0, 0 };
        
        static std::size_t measure(std::size_t page_size);

        template <typename T> static std::size_t safeSizeOf(T buf)
        {
            auto _buf = buf;
            _buf += super_t::baseSize();
            auto page_size = o_ext_space::__const_ref(buf).m_page_size;
            buf += page_size;
            return page_size;
        }
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
        ExtSpace(tag_create, DRAM_Pair);
        ExtSpace(DRAM_Pair, AccessType);
        ~ExtSpace();
        
        bool operator!() const;
        
        // get the primary REL_Index
        inline REL_Index &getREL_Index() {
            assert(m_rel_index);
            return *m_rel_index;
        }
        
        void refresh();
        void commit();
        
    private:
        std::shared_ptr<DRAM_Prefix> m_dram_prefix;
        std::shared_ptr<DRAM_Allocator> m_dram_allocator;
        mutable Memspace m_dram_space;
        const AccessType m_access_type;
        // the root object (created at address 0)
        db0::v_object<o_ext_space> m_ext_space_root;
        std::unique_ptr<REL_Index> m_rel_index;
        
        db0::v_object<o_ext_space> tryOpenRoot() const;
        std::unique_ptr<REL_Index> tryOpenPrimaryREL_Index(AccessType) const;
    };
    
}