#include <dbzero/core/memory/MemLock.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <stdexcept>

namespace db0

{

    MemLock::MemLock(void *buffer, std::shared_ptr<ResourceLock> lock)
        : m_buffer(buffer)
        , m_lock(lock)
    {
        assert(m_buffer);
    }
    
    void *MemLock::modify(std::size_t offset, std::size_t size) {
        THROWF(db0::InternalException) << "not implemented" << THROWF_END;
    }

    void *MemLock::modify()
    {
        m_lock->setDirty();
        return m_buffer;
    }
    
    void MemLock::release()
    {
        m_lock = nullptr;
        m_buffer = nullptr;
    }    

    MemLock MemLock::getSubrange(std::size_t offset) const {
        return { const_cast<std::byte*>(static_cast<const std::byte*>(m_buffer)) + offset, m_lock };
    }
    
    bool MemLock::operator==(const MemLock &other) const {
        return m_buffer == other.m_buffer && m_lock == other.m_lock;
    }
    
    bool MemLock::operator!=(const MemLock &other) const {
        return m_buffer != other.m_buffer || m_lock != other.m_lock;
    }

    unsigned int MemLock::use_count() const {
        return m_lock.use_count();
    }
    
    void MemLock::operator=(MemLock &&other)
    {
        m_buffer = other.m_buffer;
        m_lock = std::move(other.m_lock);        
        other.m_buffer = nullptr;
    }

}