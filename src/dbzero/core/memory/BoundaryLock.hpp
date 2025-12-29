// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "ResourceLock.hpp"
#include "DP_Lock.hpp"
#include <unordered_map>

namespace db0

{

    /**
     * BoundaryLock is supported by the 2 underlying DP_Lock-s
    */
    class BoundaryLock: public ResourceLock
    {
    public:
        BoundaryLock(StorageContext, std::uint64_t address, std::shared_ptr<DP_Lock> lhs, std::size_t lhs_size,
            std::shared_ptr<DP_Lock> rhs, std::size_t rhs_size, FlagSet<AccessOptions>);
        // Create copy of an existing BoundaryLock (for CoW)
        BoundaryLock(StorageContext, std::uint64_t address, const BoundaryLock &lock, std::shared_ptr<DP_Lock> lhs, std::size_t lhs_size,
            std::shared_ptr<DP_Lock> rhs, std::size_t rhs_size, FlagSet<AccessOptions>);
        
        virtual ~BoundaryLock();
        
        // flush the boundary writes only (without flushing parent locks)
        void flushBoundary();

        bool tryFlush(FlushMethod) override;

        void flush() override;

        // rebase parent locks if needed
        void rebase(const std::unordered_map<const ResourceLock*, std::shared_ptr<DP_Lock> > &rebase_map);

#ifndef NDEBUG
        bool isBoundaryLock() const override;
#endif

    private:
        std::shared_ptr<DP_Lock> m_lhs;
        const std::size_t m_lhs_size;
        std::shared_ptr<DP_Lock> m_rhs;
        const std::size_t m_rhs_size;

        bool _tryFlush(FlushMethod);
    };
            
}