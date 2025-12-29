// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <memory>
#include <cassert>
#include <optional>
#include <dbzero/core/memory/Prefix.hpp>
#include <dbzero/core/memory/Allocator.hpp>
#include <dbzero/core/memory/mptr.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>

namespace db0

{
    
    class ProcessTimer;
    class vtypeless;
    class GC0;

    /**
     * Combines application requisites, prefix related 
     * typically the Prefix instance with the corresponding Allocator
    */
    class Memspace
    {
    public:
        Memspace() = default;

        Memspace(std::shared_ptr<Prefix> prefix, std::shared_ptr<Allocator> allocator,
            std::optional<std::uint64_t> uuid = {});
        
        struct tag_from_reference {};
        Memspace(tag_from_reference, std::shared_ptr<Prefix> prefix, Allocator &allocator,
            std::optional<std::uint64_t> uuid = {});

        virtual ~Memspace() = default;

        void init(std::shared_ptr<Prefix> prefix, std::shared_ptr<Allocator> allocator);

        inline mptr myPtr(Address address, FlagSet<AccessOptions> access_mode = {}) {
            return mptr(*this, address, access_mode);
        }
        
        inline const Allocator &getAllocator() const {
            assert(m_allocator_ptr);
            return *m_allocator_ptr;
        }
        
        // Memspace::alloc implements the auto-align logic
        Address alloc(std::size_t size, std::uint32_t slot_num = 0, unsigned char realm_id = 0, 
            unsigned char locality = 0);
        UniqueAddress allocUnique(std::size_t size, std::uint32_t slot_num = 0, unsigned char realm_id = 0, 
            unsigned char locality = 0);
        
        void free(Address);

        inline Prefix &getPrefix() const {
            return *m_prefix;
        }

        std::shared_ptr<Prefix> getPrefixPtr() const {
            return m_prefix;
        }

        bool operator==(const Memspace &) const;
        bool operator!=(const Memspace &) const;

        std::size_t getPageSize() const;
        
        /**
         * Commit data with backend and immediately initiate a new transaction
         * @return true if the transaction state number was changed
        */
        bool commit(ProcessTimer * = nullptr);

        // Detach memspace associated / owned resources (e.g. Allocator)
        void detach() const;

        /**
         * Close this memspace, drop uncommited data
        */
        void close(ProcessTimer * = nullptr);

        bool isClosed() const;
        
        AccessType getAccessType() const;

        bool beginRefresh();
        
        /**
         * Refresh the memspace to the latest state (e.g. after updates done by other processes)
         * Operation only allowed for read-only memspaces
         * @return true if the memspace was updated
        */
        void completeRefresh();

        std::uint64_t getStateNum() const;
        
        std::uint64_t getUUID() const;
        
        void beginAtomic();
        void endAtomic();
        void cancelAtomic();
        
        inline BaseStorage &getStorage() {
            return *m_storage_ptr;
        }
        
        // Check if the address is valid (allocated) with the underlying allocator
        // and retrieve the allocation size (on request)
        bool isAddressValid(Address, unsigned char realm_id, std::size_t *size_of_result = nullptr) const;
        
        // Calcuate page number for a specific address (not validated)
        inline std::uint64_t getPageNum(Address address) const {
            // NOTE: m_page_shift is 0 if page size is not a power of 2
            return m_page_shift ? (address.getOffset() >> m_page_shift) : (address.getOffset() / m_page_size);
        }

        void collectForFlush(db0::vtypeless *vptr) {
            m_maybe_need_flush.push_back(vptr);
        }
        
        void collectModified(db0::vtypeless *vptr) {
            m_maybe_modified.push_back(vptr);
        }
        
    protected:
        std::shared_ptr<Prefix> m_prefix;
        BaseStorage *m_storage_ptr = nullptr;
        std::shared_ptr<Allocator> m_allocator;
        Allocator *m_allocator_ptr = nullptr;
        // UUID (if passed from a derived class)
        std::optional<std::uint64_t> m_derived_UUID;
        // flag indicating if the atomic operation is in progress
        bool m_atomic = false;
        std::size_t m_page_size = 0;
        unsigned int m_page_shift = 0;
        // exhaustive list of instances which may need flush
        std::vector<db0::vtypeless*> m_maybe_need_flush;
        // exhaustive list of pointers to instances (may be expired!) modified within the current transaction
        std::vector<db0::vtypeless*> m_maybe_modified;
        
        inline Allocator &getAllocatorForUpdate() {
            assert(m_allocator_ptr);
            return *m_allocator_ptr;
        }

        const std::vector<db0::vtypeless*> &getModified() const {
            return m_maybe_modified;
        }
        
        const std::vector<db0::vtypeless*> &getForFlush() const {
            return m_maybe_need_flush;
        }
    };
    
}
