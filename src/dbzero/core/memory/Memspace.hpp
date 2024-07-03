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
    
    /**
     * Combines application requisites, prefix related 
     * typically the Prefix instance with the corresponding Allocator
    */
    class Memspace
    {
    public:
        Memspace() = default;

        Memspace(std::shared_ptr<Prefix> prefix, std::shared_ptr<Allocator> allocator,
            std::optional<std::uint64_t> uuid = {})
            : m_prefix(prefix)
            , m_storage_ptr(&prefix->getStorage())
            , m_allocator(allocator)
            , m_allocator_ptr(m_allocator.get())
            , m_derived_UUID(uuid)
        {
        }
        
        virtual ~Memspace() = default;
        
        struct tag_from_reference {};
        Memspace(tag_from_reference, std::shared_ptr<Prefix> prefix, Allocator &allocator,
            std::optional<std::uint64_t> uuid = {})
            : m_prefix(prefix)
            , m_storage_ptr(&prefix->getStorage())
            , m_allocator_ptr(&allocator)
            , m_derived_UUID(uuid)
        {
        }

        void init(std::shared_ptr<Prefix> prefix, std::shared_ptr<Allocator> allocator);

        inline mptr myPtr(std::uint64_t address, FlagSet<AccessOptions> access_mode = {}) {
            return mptr(*this, address, access_mode);
        }
        
        inline Allocator &getAllocator() {
            assert(m_allocator_ptr);
            return *m_allocator_ptr;
        }

        inline const Allocator &getAllocator() const {
            assert(m_allocator_ptr);
            return *m_allocator_ptr;
        }

        inline Prefix &getPrefix() {
            return *m_prefix;
        }

        inline const Prefix &getPrefix() const {
            return *m_prefix;
        }

        std::shared_ptr<Prefix> getPrefixPtr() {
            return m_prefix;
        }

        bool operator==(const Memspace &) const;
        bool operator!=(const Memspace &) const;

        std::size_t getPageSize() const;
        
        /**
         * Commit data with backend and immediately initiate a new transaction
        */
        void commit();

        /**
         * Close this memspace, drop uncommited data
        */
        void close();
        
        AccessType getAccessType() const;

        /**
         * Refresh the memspace to the latest state (e.g. after updates done by other processes)
         * Operation only allowed for read-only memspaces
         * @return true if the memspace was updated
        */
        bool refresh();

        std::uint64_t getStateNum() const;
        
        std::uint64_t getUUID() const;
        
        void beginAtomic();
        void endAtomic();
        void cancelAtomic();

        inline BaseStorage &getStorage() {
            return *m_storage_ptr;
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
    };

}
