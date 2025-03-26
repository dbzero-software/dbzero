#include <dbzero/core/memory/MemLock.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <stdexcept>

namespace db0

{

    MemLock::MemLock(void *buffer, std::shared_ptr<ResourceLock> lock)
        : m_buffer(buffer)
        , m_lock(lock)
        , m_lock_ptr(lock.get())
    {
        assert(m_buffer);
    }
    
    void *MemLock::modify()
    {
        assert(m_lock_ptr);
        m_lock_ptr->setDirty();
        return m_buffer;
    }
    
    void MemLock::onDirtyCallback()
    {
        assert(m_lock_ptr);
        m_lock_ptr->onDirtyCallback();
    }

    void MemLock::release()
    {
        m_lock = nullptr;
        m_lock_ptr = nullptr;
        m_buffer = nullptr;
    }    
    
    void MemLock::discard()
    {
        if (m_lock) {
            m_lock->discard();
        }
        release();
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
        m_lock_ptr = m_lock.get();     
        other.m_buffer = nullptr;
    }

    std::shared_ptr<ResourceLock> MemLock::lock() const {
        return m_lock;
    }
    
}