#pragma once

#include "Prefix.hpp"
#include "SlabAllocator.hpp"
#include "AlgoAllocator.hpp"
#include "Allocator.hpp"
#include <deque>
#include <unordered_map>
#include <functional>
#include <dbzero/core/serialization/Fixed.hpp>
    
namespace db0

{

    class SlabRecycler;
    class SlabManager;

    struct [[gnu::packed]] o_meta_header: public o_fixed<o_meta_header> 
    {
        // page size in bytes
        std::uint32_t m_page_size;
        // slab size in bytes
        std::uint32_t m_slab_size;
        std::uint64_t m_slab_defs_ptr;
        std::uint64_t m_capacity_items_ptr;
        
        o_meta_header(std::uint32_t page_size, std::uint32_t slab_size, std::uint64_t slab_defs_ptr, std::uint64_t capacity_items_ptr)
            : m_page_size(page_size)
            , m_slab_size(slab_size)
            , m_slab_defs_ptr(slab_defs_ptr)
            , m_capacity_items_ptr(capacity_items_ptr)
        {
        }
    };
    
    class MetaAllocator: public Allocator
    {
    public:
        /**
         * Opens an existing instance of a MetaAllocator over a specific prefix
        */
        MetaAllocator(std::shared_ptr<Prefix> prefix, SlabRecycler *recycler = nullptr);
        
        virtual ~MetaAllocator();

        /**
         * Initialize a new MetaAllocator instance over an empty prefix
        */
        static void formatPrefix(std::shared_ptr<Prefix> prefix, std::size_t page_size, std::size_t slab_size);
        
        struct [[gnu::packed]] CapacityItem
        {
            std::uint32_t m_remaining_capacity;
            std::uint32_t m_slab_id;
            
            CapacityItem() = default;

            CapacityItem(std::uint32_t remaining_capacity, std::uint32_t slab_id)
                : m_remaining_capacity(remaining_capacity)
                , m_slab_id(slab_id)
            {
            }

            // note descending order of comparisons
            struct CompT
            {
                inline bool operator()(const CapacityItem &lhs, const CapacityItem &rhs) const {
                    if (lhs.m_remaining_capacity == rhs.m_remaining_capacity)
                        return lhs.m_slab_id < rhs.m_slab_id;
                    return rhs.m_remaining_capacity < lhs.m_remaining_capacity;
                }
                
                inline bool operator()(const CapacityItem &lhs, std::uint32_t rhs) const {
                    return rhs < lhs.m_remaining_capacity;
                }

                inline bool operator()(std::uint32_t lhs, const CapacityItem &rhs) const {
                    return rhs.m_remaining_capacity < lhs;
                }
            };

            struct EqualT
            {
                inline bool operator()(const CapacityItem &lhs, const CapacityItem &rhs) const {
                    return lhs.m_remaining_capacity == rhs.m_remaining_capacity && lhs.m_slab_id == rhs.m_slab_id;
                }
                
                inline bool operator()(const CapacityItem &lhs, std::uint32_t rhs) const {
                    return lhs.m_remaining_capacity == rhs;
                }

                inline bool operator()(std::uint32_t lhs, const CapacityItem &rhs) const {
                    return lhs == rhs.m_remaining_capacity;
                }
            };
        };

        struct [[gnu::packed]] SlabDef
        {
            std::uint32_t m_slab_id;
            std::uint32_t m_remaining_capacity;
            
            SlabDef(std::uint32_t slab_id, std::uint32_t remaining_capacity)
                : m_slab_id(slab_id)
                , m_remaining_capacity(remaining_capacity)
            {
            }

            struct CompT
            {
                inline bool operator()(const SlabDef &lhs, const SlabDef &rhs) const {                    
                    return lhs.m_slab_id < rhs.m_slab_id;                    
                }
                
                inline bool operator()(const SlabDef &lhs, std::uint32_t rhs) const {
                    return lhs.m_slab_id < rhs;
                }

                inline bool operator()(std::uint32_t lhs, const SlabDef &rhs) const {
                    return lhs < rhs.m_slab_id;
                }
            };

            struct EqualT
            {
                inline bool operator()(const SlabDef &lhs, const SlabDef &rhs) const {
                    return lhs.m_slab_id == rhs.m_slab_id;
                }
                
                inline bool operator()(const SlabDef &lhs, std::uint32_t rhs) const {
                    return lhs.m_slab_id == rhs;
                }

                inline bool operator()(std::uint32_t lhs, const SlabDef &rhs) const {
                    return lhs == rhs.m_slab_id;
                }
            };
        };

        using CapacityTreeT = SGB_Tree<CapacityItem, CapacityItem::CompT, CapacityItem::EqualT>;
        using SlabTreeT = SGB_Tree<SlabDef, SlabDef::CompT, SlabDef::EqualT>;
        
        std::optional<std::uint64_t> tryAlloc(std::size_t size, std::uint32_t slot_num = 0) override;
        
        void free(std::uint64_t address) override;

        std::size_t getAllocSize(std::uint64_t address) const override;
            
        void commit() override;
        
        /**
         * Calculate the number of slabs which can be annotated by a single page pair
        */
        static std::size_t getSlabCount(std::size_t page_size, std::size_t slab_size);

        static std::function<std::uint64_t(unsigned int)> getAddressPool(std::size_t offset, std::size_t page_size,
            std::size_t slab_size);

        static std::function<unsigned int(std::uint64_t)> getReverseAddressPool(std::size_t offset, std::size_t page_size,
            std::size_t slab_size);

        static std::function<std::uint32_t(std::uint64_t)> getSlabIdFunction(std::size_t offset, std::size_t page_size,
            std::size_t slab_size);
        
        /**
         * Calculate slab ID for the given address
        */
        std::uint32_t getSlabId(std::uint64_t address) const;
        
        unsigned int getSlabCount() const;
        
        /**
         * Retrieve information about the remaining space available to the Slab
        */
        std::uint32_t getRemainingCapacity(std::uint32_t slab_id) const;

        /**
         * Retrieve a new slab reserved for private use
         * note that this slab will not be available for allocations from MetaAllocator and has to be used directly
        */
        std::shared_ptr<SlabAllocator> reserveNewSlab();

        /**
         * Open existing slab for private use (reserved slab)
        */
        std::shared_ptr<SlabAllocator> openReservedSlab(std::uint64_t address, std::size_t size) const;

        /**
         * Close the allocator and flush all modifications with backed
        */
        void close();
        
        /**
         * Get address of the 1st allocation (irrespective of whether it was performed by the MetaAllocator or not)
        */
        std::uint64_t getFirstAddress() const;
        
        SlabRecycler *getSlabRecyclerPtr() const;

    private:
        std::shared_ptr<Prefix> m_prefix;
        o_meta_header m_header;
        mutable AlgoAllocator m_algo_allocator;
        Memspace m_metaspace;
        SlabTreeT m_slab_defs;
        // slab defs ordered by capacity descending
        CapacityTreeT m_capacity_items;
        std::unique_ptr<SlabManager> m_slab_manager;
        SlabRecycler *m_recycler_ptr;
        std::function<std::uint32_t(std::uint64_t)> m_slab_id_function;
        
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
    };

}

namespace std 

{
    
    ostream &operator<<(ostream &os, const db0::MetaAllocator::CapacityItem &item);

    ostream &operator<<(ostream &os, const db0::MetaAllocator::SlabDef &item);

}