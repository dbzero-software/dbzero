#pragma once

#include <unordered_map>
#include <functional>
#include <dbzero/core/memory/Prefix.hpp>
#include <dbzero/core/memory/DP_Lock.hpp>
#include <dbzero/core/storage/Storage0.hpp>
#include <optional>

namespace db0

{

    class BaseStorage;
    
    /**
     * The in-memory only prefix implementation
     * access is limited to page or sub-page ranges
    */
    class DRAM_Prefix: public Prefix, public std::enable_shared_from_this<Prefix>
    {
    public:        
        // A function to consume a single memory page (for serialization)
        // is_last=true is set for the last flushed page
        using SinkFunction = std::function<void(std::uint64_t page_num, const void *)>;

        DRAM_Prefix(std::size_t page_size);
        virtual ~DRAM_Prefix();

        MemLock mapRange(std::uint64_t address, std::size_t size, FlagSet<AccessOptions> = {}) override;
        
        std::uint64_t getStateNum() const override;
        
        std::uint64_t commit() override;

        void close() override;
        
        std::uint64_t refresh() override;

        std::size_t getPageSize() const;
        
        /**
         * Output all modified pages in to a user provided sink function
         * and mark pages as non-dirty
         * The flush order is undefined
        */
        void flushDirty(SinkFunction) const;
        
        /**
         * Set or upddate a single page
        */
        void update(std::size_t page_num, const void *bytes, bool mark_dirty = true);

        bool empty() const;
        
        /**
         * Copy all contents of another prefix to this one, dirty flag not set or updated
         * Existing pages not present in the other prefix will be removed
        */
        void operator=(const DRAM_Prefix &);

        AccessType getAccessType() const override
        {
            return AccessType::READ_WRITE;
        }

        std::uint64_t getLastUpdated() const override;

        std::shared_ptr<Prefix> getSnapshot(std::optional<std::uint64_t> state_num = {}) const override;
        
        BaseStorage &getStorage() const override;
        
    private:
        const std::size_t m_page_size;        
        mutable Storage0 m_dev_null;

        struct MemoryPage
        {
            mutable std::shared_ptr<DP_Lock> m_lock;
            void *m_buffer;
            
            MemoryPage(BaseStorage &, std::uint64_t address, std::size_t size);
            void resetDirtyFlag();
        };

        mutable std::unordered_map<std::size_t, MemoryPage> m_pages;
    };
    
}