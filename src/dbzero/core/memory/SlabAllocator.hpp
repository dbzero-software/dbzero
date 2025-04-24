#pragma once

#include "Allocator.hpp"
#include "Prefix.hpp"
#include "BitSpace.hpp"
#include "Memspace.hpp"
#include <dbzero/core/crdt/CRDT_Allocator.hpp>
#include <dbzero/core/serialization/Fixed.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/collections/vector/LimitedVector.hpp>

namespace db0

{
    
    struct [[gnu::packed]] o_slab_header: public db0::o_fixed<o_slab_header>
    {
        const std::uint32_t m_version = 1;
        // number of bytes available to the underlying CRDT alllocator
        // this value is changed dynamically
        std::uint32_t m_size = 0;
        // relative pointers (relative to the beginning of the slab)
        std::uint32_t m_alloc_set_ptr = 0;
        std::uint32_t m_blank_set_ptr = 0;
        std::uint32_t m_aligned_blank_set_ptr = 0;
        std::uint32_t m_stripe_set_ptr = 0;
        std::uint32_t m_alloc_counter_ptr = 0;
        std::uint32_t m_reserved[2] = {0};

        o_slab_header() = default;
        
        o_slab_header(std::uint32_t size, std::uint32_t alloc_set_ptr, std::uint32_t blank_set_ptr, 
            std::uint32_t aligned_blank_set_ptr, std::uint32_t stripe_set_ptr, std::uint32_t alloc_counter_ptr)
            : m_size(size)
            , m_alloc_set_ptr(alloc_set_ptr)
            , m_blank_set_ptr(blank_set_ptr)
            , m_aligned_blank_set_ptr(aligned_blank_set_ptr)
            , m_stripe_set_ptr(stripe_set_ptr)        
            , m_alloc_counter_ptr(alloc_counter_ptr)
        {
        }
    };
    
    static constexpr unsigned int SLAB_BITSPACE_SIZE() {
        // typical configuration, sufficient for a 64MB slab
        // FIXME: page_size hardcoded
        return 64 * 1024 * 1024 / 4096;
    }

    /**
     * The SlabAllocator takes a fixed size address range (e.g. 64MB)
     * and organizes the space with the use of BitSetAllocator/BitSpace + CRDT_Allocator 
     * providing a clean Allocator interface
    */
    class SlabAllocator: public Allocator
    {
    public:
        /**
         * @param prefix the prefix where the slab is stored
         * @param begin_addr the address where the slab starts
         * @param size the size of the slab (in bytes)
         * @param page_size the size of a single page used by the underlying BitSpace / BitSetAllocator
         * @param remaining_capacity the remaining capacity if known
        */
        SlabAllocator(std::shared_ptr<Prefix> prefix, std::uint64_t begin_addr, std::uint32_t size, std::size_t page_size,
            std::optional<std::size_t> remaining_capacity = {});

        virtual ~SlabAllocator();
        
        std::optional<Address> tryAlloc(std::size_t size, std::uint32_t slot_num = 0,
            bool aligned = false) override;

        std::optional<UniqueAddress> tryAllocUnique(std::size_t size, std::uint32_t slot_num = 0,
            bool aligned = false) override;
                
        void free(Address) override;

        std::size_t getAllocSize(Address) const override;
        
        bool isAllocated(Address) const override;
        
        void commit() const override;

        void detach() const override;
        
        bool inRange(std::uint64_t address) const override;
        
        /**
         * Initialize a new allocator over a specific slab
         * 
         * @return the created slab's capacity in bytes
        */
        static std::size_t formatSlab(std::shared_ptr<Prefix> prefix, Address begin_addr, std::uint32_t size,
            std::size_t page_size);
        
        const std::size_t getSlabSize() const;
        
        /**
         * Get number of bytes occupied by the administrative data structures
         * this number varies depending on the number and size of allocated objects
        */
        const std::size_t getAdminSpaceSize(bool include_margin) const;
        
        /**
         * Calculate the initial administrative space size given specific criteria
        */
        static std::size_t calculateAdminSpaceSize(std::size_t page_size);
        
        /**
         * Get maximum theoretical size of a single allocation possible on this type of slab when unoccupied
        */
        std::size_t getMaxAllocSize() const;
        
        /**
         * Get estimated remaining capacity (if initialized with remaining capacity, otherwise throws)
        */
        std::size_t getRemainingCapacity() const;
        
        const Prefix &getPrefix() const;

        /**
         * Register a handler to be called pre-destruction
        */
        void setOnCloseHandler(std::function<void(const SlabAllocator &)>);

        bool empty() const;

        /**
         * Get the slab's address
        */
        std::uint64_t getAddress() const;

        std::uint32_t size() const;

        /**
         * Get address of the 1st allocation
        */
        static Address getFirstAddress();

        // SlabAllocator specific address conversions        
        inline std::uint64_t makeAbsolute(std::uint32_t relative) const {
            return m_begin_addr + relative;
        }

        inline std::uint32_t makeRelative(std::uint64_t absolute) const {
            return absolute - m_begin_addr;
        }
                
    private:
        using AllocSetT = db0::CRDT_Allocator::AllocSetT;
        using BlankSetT = db0::CRDT_Allocator::BlankSetT;
        using AlignedBlankSetT = db0::CRDT_Allocator::AlignedBlankSetT;
        using StripeSetT = db0::CRDT_Allocator::StripeSetT;
        using CompT = typename AlignedBlankSetT::CompT;
        using LimitedVectorT = LimitedVector<std::uint16_t>;
        
        // the number of administrative area pages
        // we determined this number to reflect the number of underlying extensible collections
        // and based on the assumption that each of them needs at least 1 page for expansion
        static constexpr std::uint32_t ADMIN_SPAN() {            
            return 5;
        }
        
        std::shared_ptr<Prefix> m_prefix;
        const std::uint64_t m_begin_addr;
        const std::size_t m_page_size;
        const std::uint32_t m_page_shift;
        const std::uint32_t m_slab_size;
        Memspace m_internal_memspace;
        v_object<o_slab_header> m_header;
        BitSpace<SLAB_BITSPACE_SIZE()> m_bitspace;
        AllocSetT m_allocs;
        BlankSetT m_blanks;
        AlignedBlankSetT m_aligned_blanks;
        StripeSetT m_stripes;
        // a dedicated storage for page-level instance ID counters
        LimitedVectorT m_alloc_counter;
        CRDT_Allocator m_allocator;
        const std::optional<std::size_t> m_initial_remaining_capacity;
        std::size_t m_initial_admin_size;        
        std::function<void(const SlabAllocator &)> m_on_close_handler;
        
        static std::uint64_t headerAddr(std::uint64_t begin_addr, std::uint32_t size);
    };
    
}
