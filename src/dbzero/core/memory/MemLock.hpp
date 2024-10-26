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

        /**
         * Mark a specific part of the buffer as modified
         * 
         * Mark a specific part of the buffer (e.g. a single variable) as modified.
         * This method does not modify any data but only marks as modified.
         * 
         * @param offset offset in bytes (must be within the mapped range). validated in debug mode only
         * @param size size in bytes (must be within the mapped range). validated in debug mode only
         * @return the pointer to the modified range
        */
        void *modify(std::size_t offset, std::size_t size);

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