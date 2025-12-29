// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ExtSpace.hpp"

namespace db0

{
    
    o_ext_space::o_ext_space(std::uint32_t page_size)
        : m_page_size(page_size)
    {
        assert(page_size >= sizeof(*this));
        // initialize reserved area to zero
        std::memset((std::byte*)this + sizeof(*this), 0, page_size - sizeof(*this));
    }

    std::size_t o_ext_space::measure(std::size_t page_size) {
        return page_size;
    }
    
    ExtSpace::ExtSpace(tag_create, DRAM_Pair dram_pair)
        : m_dram_prefix(dram_pair.first)
        , m_dram_allocator(dram_pair.second)
        , m_dram_space(DRAMSpace::create(dram_pair))
        , m_access_type(AccessType::READ_WRITE)
        , m_ext_space_root(m_dram_space, m_dram_space.getPageSize())
        , m_rel_index(std::make_unique<REL_Index>(m_dram_space, m_dram_space.getPageSize(), AccessType::READ_WRITE))
    {
        assert(!!m_ext_space_root);
        assert(m_rel_index);
        // make sure root is the first allocation
        assert(m_ext_space_root.getAddress() == m_dram_allocator->firstAlloc());
        // NOTE: the secondary REL_Index is not used currently
        m_ext_space_root.modify().m_rel_index_addr[0] = m_rel_index->getAddress();
    }
    
    ExtSpace::ExtSpace(DRAM_Pair dram_pair, AccessType access_type)
        : m_dram_prefix(dram_pair.first)
        , m_dram_allocator(dram_pair.second)
        , m_dram_space(DRAMSpace::tryCreate(dram_pair))
        , m_access_type(access_type)
        , m_ext_space_root(tryOpenRoot())
        , m_rel_index(tryOpenPrimaryREL_Index(access_type))
    {
    }
    
    ExtSpace::~ExtSpace()
    {
    }
    
    void ExtSpace::refresh()
    {        
        m_ext_space_root.detach();
        if (m_rel_index) {
            m_rel_index->refresh();
        }
    }
    
    void ExtSpace::commit()
    {
        if (!!m_ext_space_root) {
            m_ext_space_root.commit();
        }
        if (m_rel_index) {
            m_rel_index->commit();
        }
    }
    
    db0::v_object<o_ext_space> ExtSpace::tryOpenRoot() const
    {
        if (!(*this)) {
            return {};
        }
        // retrieve root from the first allocation
        return db0::v_object<o_ext_space>(m_dram_space.myPtr(m_dram_allocator->firstAlloc()));
    }
    
    std::unique_ptr<REL_Index> ExtSpace::tryOpenPrimaryREL_Index(AccessType access_type) const
    {
        if (!(*this)) {        
            return {};
        }
        auto rel_index_addr = Address::fromOffset(m_ext_space_root->m_rel_index_addr[0]);
        return std::make_unique<REL_Index>(
            m_dram_space.myPtr(rel_index_addr), m_dram_space.getPageSize(), access_type
        );
    }
    
    std::unique_ptr<ExtSpace::const_iterator> ExtSpace::tryBegin() const
    {
        if (!(*this) || !m_rel_index) {
            return {};
        }
        return std::make_unique<const_iterator>(m_rel_index->cbegin());
    }
    
}