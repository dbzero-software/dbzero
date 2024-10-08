#pragma once

#include <memory>
#include <vector>
#include <mutex>
#include <functional>
#include "ResourceLock.hpp"

namespace db0

{

    // A cache specialized in storing dirty locks
    struct DirtyCache
    {        
        // A function to consume a single resource (for serialization)
        using SinkFunction = std::function<void(std::uint64_t page_num, const void *)>;

        DirtyCache(std::size_t page_size);

        mutable std::mutex m_mutex;
        std::vector<std::shared_ptr<ResourceLock> > m_locks;
        const unsigned int m_shift;

        // register resource with the dirty locks
        void append(std::shared_ptr<ResourceLock>);
        void flush();

        /**
         * Output all modified pages in to a user provided sink function
         * and mark pages as non-dirty
         * The flush order is undefined
        */
        void flushDirty(SinkFunction);
    };

} 