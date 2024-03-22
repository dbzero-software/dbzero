#include "Memspace.hpp"

namespace db0

{

    bool Memspace::operator==(const Memspace &other) const
    {
        return m_prefix == other.m_prefix;
    }
    
    bool Memspace::operator!=(const Memspace &other) const
    {
        return m_prefix != other.m_prefix;
    }
    
    void Memspace::init(std::shared_ptr<Prefix> prefix, std::shared_ptr<Allocator> allocator)
    {
        m_prefix = prefix;
        m_allocator = allocator;
        m_allocator_ptr = m_allocator.get();
    }

    std::size_t Memspace::getPageSize() const
    {
        return m_prefix->getPageSize();
    }
    
    void Memspace::commit()
    {
        assert(m_prefix);
        // prepare the allocator for the next transaction
        getAllocator().commit();
        m_prefix->commit();
    }
    
    void Memspace::close()
    {
        m_allocator_ptr = nullptr;
        m_allocator = nullptr;
        m_prefix->close();
        m_prefix = nullptr;
    }
    
    AccessType Memspace::getAccessType() const
    {
        assert(m_prefix);
        return m_prefix->getAccessType();
    }
    
    bool Memspace::refresh()
    {
        assert(getAccessType() == AccessType::READ_ONLY);
        return m_prefix->refresh();
    }
    
    std::uint64_t Memspace::getStateNum() const
    {
        assert(m_prefix);
        return m_prefix->getStateNum();
    }

}