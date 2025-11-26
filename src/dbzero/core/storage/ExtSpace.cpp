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
    
    bool ExtSpace::operator!() const {
        return !m_dram_prefix || !m_dram_allocator;
    }
    
    db0::v_object<o_ext_space> ExtSpace::tryOpenRoot() const
    {
        if (!(*this)) {
            return {};
        }
        return db0::v_object<o_ext_space>(m_dram_space.myPtr(Address::fromOffset(0)));
    }
    
    std::unique_ptr<REL_Index> ExtSpace::tryOpenPrimaryREL_Index(AccessType access_type) const
    {
        if (!m_ext_space_root) {
            return {};
        }
        auto rel_index_addr = Address::fromOffset(m_ext_space_root->m_rel_index_addr[0]);
        return std::make_unique<REL_Index>(
            m_dram_space.myPtr(rel_index_addr), m_dram_space.getPageSize(), access_type
        );
    }
    
}