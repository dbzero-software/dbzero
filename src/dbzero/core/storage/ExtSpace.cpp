#include "ExtSpace.hpp"

namespace db0

{
    
    o_ext_space::o_ext_space() {
        std::memset(m_reserved.data(), 0, sizeof(m_reserved));
    }

    ExtSpace::ExtSpace(tag_create, DRAM_Pair dram_pair, std::uint32_t step_size)
        : m_dram_prefix(dram_pair.first)
        , m_dram_allocator(dram_pair.second)
        , m_dram_space(DRAMSpace::create(dram_pair))
        , m_access_type(AccessType::READ_WRITE)
        , m_ext_space_root(m_dram_space)
        , m_rel_index(m_dram_space, step_size)
    {
        // NOTE: the secondary REL_Index is not used currently
        m_ext_space_root.modify().m_rel_index_addr[0] = m_rel_index.getAddress();
    }

    ExtSpace::ExtSpace(DRAM_Pair dram_pair, AccessType access_type, std::uint32_t step_size)
        : m_dram_prefix(dram_pair.first)
        , m_dram_allocator(dram_pair.second)
        , m_dram_space(DRAMSpace::create(dram_pair))
        , m_access_type(access_type)
        , m_ext_space_root(tryOpenRoot())
        , m_rel_index(tryOpenPrimaryREL_Index(step_size))
    {
    }

    ExtSpace::~ExtSpace()
    {
    }
    
    db0::v_object<o_ext_space> ExtSpace::tryOpenRoot() const
    {
        if (!m_dram_prefix || !m_dram_allocator) {
            return {};
        }
        return db0::v_object<o_ext_space>(m_dram_space.myPtr(Address::fromOffset(0)));
    }
    
    REL_Index ExtSpace::tryOpenPrimaryREL_Index(std::uint32_t step_size) const
    {
        if (!m_ext_space_root) {
            return {};
        }
        return { m_dram_space.myPtr(m_ext_space_root->m_rel_index_addr[0]), step_size };
    }

}