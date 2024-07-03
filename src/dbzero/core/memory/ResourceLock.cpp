#include <dbzero/core/memory/ResourceLock.hpp>
#include <iostream>
#include <cstring>
#include <cassert>
#include <dbzero/core/storage/BaseStorage.hpp>

namespace db0

{
    
    ResourceLock::ResourceLock(BaseStorage &storage, std::uint64_t address, std::size_t size, FlagSet<AccessOptions> access_mode,
        std::uint64_t read_state_num, std::uint64_t write_state_num , bool create_new)
        : BaseLock(storage, address, size, access_mode, create_new)
        , m_state_num(std::max(read_state_num, write_state_num))
    {
        assert(addrPageAligned(m_storage));
        // initialzie the local buffer
        if (access_mode[AccessOptions::read]) {
            assert(read_state_num > 0);
            // read into the local buffer
            storage.read(m_address, read_state_num, m_data.size(), m_data.data(), access_mode);
        }
    }
    
    ResourceLock::ResourceLock(const ResourceLock &other, std::uint64_t write_state_num, FlagSet<AccessOptions> access_mode)
        : BaseLock(other, access_mode)
        , m_state_num(write_state_num)
    {
        assert(addrPageAligned(m_storage));
        assert(m_state_num > 0);
    }
    
    void ResourceLock::flush()
    {
        using MutexT = ResourceDirtyMutexT;
        while (MutexT::__ref(m_resource_flags).get()) {
            MutexT::WriteOnlyLock lock(m_resource_flags);
            if (lock.isLocked()) {
                // write from the local buffer
                m_storage.write(m_address, m_state_num, m_data.size(), m_data.data());
                // reset the dirty flag
                lock.commit_reset();
            }
        }
    }
    
    std::uint64_t ResourceLock::getStateNum() const {
        return m_state_num;
    }
    
    void ResourceLock::updateStateNum(std::uint64_t state_num)
    {
        assert(state_num > m_state_num);
        assert(!isDirty());        
        m_state_num = state_num;
        setDirty();
    }
    
}