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
        Address alloc(std::size_t size, std::uint32_t slot_num = 0, bool unique = false);
        
        void free(std::uint64_t address);
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
        */
        void commit(ProcessTimer * = nullptr);

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
        bool isAddressValid(std::uint64_t address) const;
        bool isAddressValid(Address) const;
        
    protected:
        std::shared_ptr<Prefix> m_prefix;
        BaseStorage *m_storage_ptr = nullptr;
        std::shared_ptr<Allocator> m_allocator;
        Allocator *m_allocator_ptr = nullptr;
        // UUID (if passed from a derived class)
        std::optional<std::uint64_t> m_derived_UUID;
        // flag indicating if the atomic operation is in progress
        bool m_atomic = false;
        std::size_t m_page_size;
                
        inline Allocator &getAllocatorForUpdate() {
            assert(m_allocator_ptr);
            return *m_allocator_ptr;
        }

    };

}
