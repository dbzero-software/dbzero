// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "Prefix.hpp"
#include "SlabAllocator.hpp"
#include "SlabItem.hpp"
#include "AlgoAllocator.hpp"
#include "Allocator.hpp"
#include "Recycler.hpp"
#include <deque>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <dbzero/core/serialization/FixedVersioned.hpp>
#include <dbzero/core/serialization/Fixed.hpp>
#include <dbzero/core/memory/Address.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{

    class SlabManager;
    using SlabRecycler = db0::Recycler<SlabItem>;

DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_realm: public o_fixed_versioned<o_realm>
    {
        Address m_slab_defs_ptr;
        Address m_capacity_items_ptr;

        o_realm() = default;
        o_realm(const std::pair<Address, Address> &);
    };
DB0_PACKED_END

DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_meta_header: public o_fixed_versioned<o_meta_header>
    {
        // NOTE: when needed, this values can be changed to 4 (or 8?) or 1 (no realms)
        static constexpr std::size_t NUM_REALMS = 2;
        // page size in bytes
        std::uint32_t m_page_size;
        // slab size in bytes
        std::uint32_t m_slab_size;
        o_realm m_realms[NUM_REALMS];

        o_meta_header(std::uint32_t page_size, std::uint32_t slab_size);
    };
DB0_PACKED_END

    class MetaAllocator: public Allocator
    {
    public:
        static constexpr std::size_t NUM_REALMS = o_meta_header::NUM_REALMS;
        static constexpr std::uint32_t REALM_MASK = NUM_REALMS - 1;

        /**
         * Opens an existing instance of a MetaAllocator over a specific prefix
         * @param deferred_free if true, free operations are deferred until commit (see Transactional Allocator)
        */
        MetaAllocator(std::shared_ptr<Prefix> prefix, SlabRecycler *recycler = nullptr,
            bool deferred_free = true);

        virtual ~MetaAllocator();

        /**
         * Initialize a new MetaAllocator instance over an empty prefix
        */
        static void formatPrefix(std::shared_ptr<Prefix> prefix, std::size_t page_size, std::size_t slab_size);

        using CapacityTreeT = SGB_Tree<CapacityItem, CapacityItem::CompT, CapacityItem::EqualT>;
        using SlabTreeT = SGB_Tree<SlabDef, SlabDef::CompT, SlabDef::EqualT>;

        std::optional<Address> tryAlloc(std::size_t size, SlotId slot_num = 0,
            bool aligned = false, unsigned char realm_id = 0, unsigned char locality = 0) override;

        std::optional<UniqueAddress> tryAllocUnique(std::size_t size, SlotId slot_num = 0,
            bool aligned = false, unsigned char realm_id = 0, unsigned char locality = 0) override;

        void free(Address) override;

        std::size_t getAllocSize(Address) const override;
        std::size_t getAllocSize(Address, unsigned char realm_id) const override;

        bool isAllocated(Address, std::size_t *size_of_result = nullptr) const override;
        bool isAllocated(Address, unsigned char realm_id, std::size_t *size_of_result = nullptr) const override;

        AllocationInfo findAllocation(Address) const override;
        AllocationInfo findAllocation(Address, unsigned char realm_id) const override;

        void commit() const override;

        void detach() const override;

        // Flush any pending "deferred" free operations
        void flush() const override;

        /**
         * Calculate the number of slabs which can be annotated by a single page pair
        */
        static std::size_t getSlabCount(std::size_t page_size, std::size_t slab_size);

        static std::function<Address(unsigned int)> getAddressPool(std::size_t offset, std::size_t page_size,
            std::size_t slab_size);

        static std::function<unsigned int(Address)> getReverseAddressPool(std::size_t offset, std::size_t page_size,
            std::size_t slab_size);

        /**
         * Fast bucketing function for raw BDevStorage byte addresses.
         *
         * This deliberately ignores MetaAllocator's internal metadata/slab layout and divides
         * the storage address space into equal-size buckets: bucket_id = (address - offset) / slab_size.
         * Use getSlabIdFunction() for real allocator slab lookup.
        */
        struct StorageSlabBucketingFunction
        {
            /**
             * Describes the storage bucket containing a raw BDevStorage byte address.
             *
             * m_slot_id is the meta-space slot used to store sparse-pair metadata for
             * pages in the bucket. m_begin_page_num and m_end_page_num form a half-open
             * logical page range [begin, end) covered by that slot.
            */
            struct Bucket
            {
                std::uint32_t m_slot_id = 0;
                std::uint64_t m_begin_page_num = 0;
                std::uint64_t m_end_page_num = 0;
            };

            std::uint64_t m_offset = 0;
            std::uint64_t m_slab_size = 0;

            std::uint32_t operator()(std::uint64_t address) const
            {
                auto rel_address = address > m_offset ? address - m_offset : 0;
                return static_cast<std::uint32_t>(rel_address / m_slab_size);
            }

            std::uint32_t operator()(Address address) const
            {
                return (*this)(address.getOffset());
            }

            /**
             * Return the bucket id plus logical page span for the bucket containing address.
             *
             * The returned page range is half-open and may be wider than the exact byte
             * range when m_offset or m_slab_size are not page-aligned.
            */
            Bucket getBucket(std::uint64_t address, std::uint32_t page_size) const
            {
                auto slot_id = (*this)(address);
                auto begin_address = m_offset + static_cast<std::uint64_t>(slot_id) * m_slab_size;
                auto end_address = begin_address + m_slab_size;
                return {
                    slot_id,
                    begin_address / page_size,
                    (end_address + page_size - 1) / page_size
                };
            }
        };

        static StorageSlabBucketingFunction getStorageSlabBucketingFunction(
            std::size_t page_size, std::size_t slab_size);

        static StorageSlabBucketingFunction getStorageSlabBucketingFunction(
            std::size_t offset, std::size_t page_size, std::size_t slab_size);

        static std::function<std::uint32_t(Address)> getSlabIdFunction(std::size_t offset, std::size_t page_size,
            std::size_t slab_size);

        unsigned int getSlabCount() const;

        /**
         * Retrieve information about the remaining space available to the Slab
        */
        std::uint32_t getRemainingCapacity(std::uint32_t slab_id) const;

        /**
         * Retrieve a new slab reserved for private use
         * note that this slab will not be available for allocations from MetaAllocator and has to be used directly
        */
        std::shared_ptr<SlabAllocator> reserveNewSlab(unsigned char realm_id = 0);

        /**
         * Open existing slab for private use (reserved slab)
        */
        std::shared_ptr<SlabAllocator> openReservedSlab(Address, std::size_t size) const;

        /**
         * Close the allocator and flush all modifications with backed
        */
        void close() override;

        /**
         * Get address of the 1st allocation (irrespective of whether it was performed by the MetaAllocator or not)
        */
        Address getFirstAddress() const;

        SlabRecycler *getSlabRecyclerPtr() const;

        // Visit all underlying slabs
        void forAllSlabs(std::function<void(const SlabAllocator &, std::uint32_t slab_id)>) const;

        // Get the number of queued defferred free operations
        std::size_t getDeferredFreeCount() const;

        // Atomic operations need special handling of the deferred free operations
        void beginAtomic();
        void endAtomic();
        void cancelAtomic();

    protected:
        // Calculate slab ID for the given address
        std::uint32_t getSlabId(Address) const;

    private:
        std::shared_ptr<Prefix> m_prefix;
        o_meta_header m_header;
        mutable AlgoAllocator m_algo_allocator;
        Memspace m_metaspace;

        struct Realm
        {
            SlabTreeT m_slab_defs;
            // slab defs ordered by capacity descending
            CapacityTreeT m_capacity_items;
            std::unique_ptr<SlabManager> m_slab_manager;

            Realm(Memspace &, std::shared_ptr<Prefix>, SlabRecycler *, o_realm, std::uint32_t slab_size,
                std::uint32_t page_size, unsigned char realm_id, bool deferred_free);

            std::uint64_t getSlabMaxAddress() const;

            void commit() const;
            void detach() const;

            SlabManager *operator->() {
                return m_slab_manager.get();
            }

            const SlabManager *operator->() const {
                return m_slab_manager.get();
            }
        };

        struct RealmsVector: protected std::vector<Realm>
        {
            RealmsVector(Memspace &, std::shared_ptr<Prefix>, SlabRecycler *, o_meta_header &,
                unsigned int size, bool deferred_free);

            // evaluate the max address from all realms
            std::uint64_t getSlabMaxAddress() const;
            std::size_t getDeferredFreeCount() const;

            inline SlabManager &operator[](unsigned char realm_id) {
                return *at(realm_id).m_slab_manager;
            }

            inline const SlabManager &operator[](unsigned char realm_id) const {
                return *at(realm_id).m_slab_manager;
            }

            void forAllSlabs(std::function<void(const SlabAllocator &, std::uint32_t)>) const;

            void detach() const;
            void commit() const;

            void beginAtomic();
            void endAtomic();
            void cancelAtomic();

            void flush() const;
            void close();
        };

        RealmsVector m_realms;
        SlabRecycler *m_recycler_ptr;
        std::function<std::uint32_t(Address)> m_slab_id_function;
        // atomic operation nesting depth
        std::uint32_t m_atomic_depth = 0;

        /**
         * Reads header information from the prefix
        */
        o_meta_header getMetaHeader(std::shared_ptr<Prefix> prefix);

        Memspace createMetaspace() const;

        /**
         * Find the slab with at least the given capacity
         * if not found then create a new slab
        */
        std::shared_ptr<SlabAllocator> getSlabAllocator(std::size_t min_capacity);

        // NOTE: instance ID will only be populated when unique = true
        std::optional<Address> tryAllocImpl(std::size_t size, SlotId slot_num, bool aligned, bool unique,
            std::uint16_t &instance_id, unsigned char realm_id, unsigned char locality);
    };

}
