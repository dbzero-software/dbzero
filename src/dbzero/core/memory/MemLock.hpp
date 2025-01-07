#pragma once

#include <memory>
#include <dbzero/core/memory/DP_Lock.hpp>

namespace db0

{

	struct MemLock
    {
        /**
         * Address of the memory buffer
        */
		void *m_buffer = nullptr;

        /**
         * Underlying locked resource
        */
		std::shared_ptr<ResourceLock> m_lock;
        
        MemLock() = default;
        MemLock(void *buffer, std::shared_ptr<ResourceLock> lock);
        MemLock(const MemLock &other) = default;

        void *modify();

        inline operator void *() const {
            return m_buffer;
        }
        
        /**
         * Release the lock, flush modifications if needed
        */
        void release();

        // Release lock, discard any changes (e.g. on rollback)
        void discard();
        
        MemLock getSubrange(std::size_t offset) const;

        MemLock &operator=(const MemLock &other) = default;
        bool operator==(const MemLock &other) const;
        bool operator!=(const MemLock &other) const;
        void operator=(MemLock &&other);
        
        /**
         * Get use count of the underlying lock
        */
        unsigned int use_count() const;
    };
        
}