// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

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
        using const_iterator = REL_Index::const_iterator;

        struct tag_create {};
        
        // NOTE: dram pair may be nullptr (for a null ExtSpace)
        ExtSpace(tag_create, DRAM_Pair);
        ExtSpace(DRAM_Pair, AccessType);
        ~ExtSpace();
        
        inline bool operator!() const {
            return !m_dram_prefix || !m_dram_allocator;
        }
        
        // Assign a mapping from an absolute to relative page number
        std::uint64_t assignRelative(std::uint64_t storage_page_num, bool is_first_in_step) {
            assert(m_rel_index);
            return m_rel_index->assignRelative(storage_page_num, is_first_in_step);
        }
        
        // Retrieve storage (absolute) page num for a given relative page num
        std::uint64_t getAbsolute(std::uint64_t rel_page_num) const {
            assert(m_rel_index);
            return m_rel_index->getAbsolute(rel_page_num);
        }
        
        std::uint64_t getRelative(std::uint64_t storage_page_num) const {
            assert(m_rel_index);
            return m_rel_index->getRelative(storage_page_num);
        }
        
        // Registers a new mapping rel_page_num -> storage_page_num
        // exception raised if unable to add the mapping
        void addMapping(std::uint64_t storage_page_num, std::uint64_t rel_page_num, std::uint32_t count) {
            assert(m_rel_index);
            m_rel_index->addMapping(storage_page_num, rel_page_num, count);
        }
        
        // Begins the iterator over sorted elements (on condition that ExtSpace is valid)
        std::unique_ptr<const_iterator> tryBegin() const;
        
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