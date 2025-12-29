// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once
    
#include "DP_Lock.hpp"

namespace db0

{

    /**
     * WideLock consists of the wide (unaligned) range + the residual lock
     * the starting address is always page-aligned
    */
    class WideLock: public DP_Lock
    {
    public:
        WideLock(StorageContext, std::uint64_t address, std::size_t size, FlagSet<AccessOptions>,
            StateNumType read_state_num, StateNumType write_state_num,
            std::shared_ptr<DP_Lock> res_lock, std::shared_ptr<ResourceLock> cow_lock = nullptr);
        
        /**
         * Create a copied-on-write lock from an existing lock
        */
        WideLock(std::shared_ptr<WideLock>, StateNumType write_state_num, FlagSet<AccessOptions> access_mode,
            std::shared_ptr<DP_Lock> res_lock);
        
        bool tryFlush(FlushMethod) override;
        void flush() override;
        
        // Flush the residual part only of the wide lock
        void flushResidual();
        
        // rebase dependent residual lock if needed
        void rebase(const std::unordered_map<const ResourceLock*, std::shared_ptr<DP_Lock> > &rebase_map);
        
        void updateStateNum(StateNumType state_num, bool is_volatile,
            std::shared_ptr<DP_Lock> res_lock);
        
#ifndef NDEBUG
        bool isBoundaryLock() const override;
#endif
        
    protected:
        friend class PrefixCache;

        // try getting diffs and adjust the cow_ptr
        bool getDiffs(const std::byte *&cow_ptr, void *buf, std::size_t size, std::vector<std::uint16_t> &result) const;
        
        const ResourceLock *getResLockPtr() const {
            return m_res_lock.get();
        }
        
    private:
        std::shared_ptr<DP_Lock> m_res_lock;
        
        bool _tryFlush(FlushMethod);
        
        void resLockFlush();
    };

}