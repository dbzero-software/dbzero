#pragma once

#include <optional>
#include <dbzero/core/memory/Memspace.hpp>
#include <dbzero/core/collections/SGB_Tree/SGB_Tree.hpp>
#include <dbzero/core/memory/utils.hpp>

namespace db0

{

    namespace crdt 
    
    {
        
        using bitarray_t = std::uint64_t;
        // static constexpr std::uint32_t SIZE_MAP[4] = { 30, 16, 8, 1 };
        static constexpr std::uint32_t SIZE_MAP[4] = { 62, 24, 8, 1 };
        static constexpr bitarray_t NSIZE = sizeof(SIZE_MAP) / sizeof(std::uint32_t);
        static constexpr int L0_CACHE_SIZE = 4;

        static constexpr bitarray_t mask(unsigned int index) {
            return ((bitarray_t)0x01 << SIZE_MAP[index]) - 1;
        };

        static constexpr bitarray_t m_masks[4] = {
            mask(0), mask(1), mask(2), mask(3)
        };
        
        static constexpr bitarray_t SIZE_MASK() {
            return (bitarray_t)0x03 << SIZE_MAP[0];
        }

        static constexpr bitarray_t NSIZE_MASK() {
            return ~SIZE_MASK();
        }
    
    }
    
    // very small buffer with N recent alloc stripes
    template <int N> class L0_Cache;

    /**
     * The CRDT Allocator implementation is based on the
     * (address, size, fill_map) + (size, address) storage model to manage the [0, size - 1] 32-bit address space
     * The interface is compatible with the db0::Allocator
    */
    class CRDT_Allocator
    {    
    public:
        // upper blank size (as the number of pages) to be registered in the aligned blank index
        // higher values allow eliminating "bind spots" in the allocator, but may incur storage & performance overhead
        static constexpr std::uint32_t ALIGNED_INDEX_THRESHOLD = 4;

        struct [[gnu::packed]] FillMap
        {            
            // the low 62 (or 30) bits are used to encode unit allocations,
            // the highest 2 bits are used to encode size (30, 16, 8, or 1)
            crdt::bitarray_t m_data = 0;

            FillMap() = default;
            
            FillMap(std::uint32_t size)
            {
                if (size == crdt::SIZE_MAP[3]) {
                    m_data = (crdt::bitarray_t)0x3 << crdt::SIZE_MAP[0];
                } else if (size == crdt::SIZE_MAP[2]) {
                    m_data = (crdt::bitarray_t)0x2 << crdt::SIZE_MAP[0];
                } else if (size == crdt::SIZE_MAP[1]) {
                    m_data = (crdt::bitarray_t)0x1 << crdt::SIZE_MAP[0];
                } else if (size == crdt::SIZE_MAP[0]) {
                    m_data = 0x0;
                } else {
                    THROWF(InternalException) << "invalid size (FillMap): " << size << THROWF_END;
                }
            }
            
            bool operator[](unsigned int index) const;

            inline std::uint32_t size() const {
                return crdt::SIZE_MAP[m_data >> crdt::SIZE_MAP[0]];
            }

            /// Tests if all units are allocated
            bool all() const;

            bool empty() const;
            
            /**
             * Allocate a single unit
             * 
             * @param hint the likely index where to allocate the unit (updated)
             * @return the index of the allocated unit or throw exception
            */
            std::uint32_t allocUnit(std::uint32_t end_index, std::uint32_t &hint_index);

            std::uint32_t allocUnit();

            std::pair<std::uint32_t, std::uint32_t> getHint() const;

            /**
             * Reset a bit at the given index
             */
            void reset(unsigned int index);

            /**
             * Try downsize by at least min_units
             * 
             * @return the number of units by which the resize was done or 0 if not resized
            */
            unsigned int tryDownsize(unsigned int min_units);

            /**
             * Get the number of unused units (high order bits)
            */
            unsigned int unused() const;

            std::uint32_t span() const;
        };

        struct Stripe;
        struct Blank;

        // 16-byte allocation record
        struct [[gnu::packed]] Alloc
        {
            std::uint32_t m_address = 0;
            std::uint32_t m_stride = 0;
            FillMap m_fill_map;
            
            Alloc() = default;
            Alloc(std::uint32_t address, std::uint32_t stride, std::uint32_t size);

            struct CompT
            {
                inline bool operator()(const Alloc &lhs, const Alloc &rhs) const {
                    return lhs.m_address < rhs.m_address;
                }
                
                inline bool operator()(const Alloc &lhs, std::uint32_t rhs) const {
                    return lhs.m_address < rhs;
                }

                inline bool operator()(std::uint32_t lhs, const Alloc &rhs) const {
                    return lhs < rhs.m_address;
                }
            };

            struct EqualT
            {
                inline bool operator()(const Alloc &lhs, const Alloc &rhs) const {
                    return lhs.m_address == rhs.m_address;
                }
                
                inline bool operator()(const Alloc &lhs, std::uint32_t rhs) const {
                    return lhs.m_address == rhs;
                }

                inline bool operator()(std::uint32_t lhs, const Alloc &rhs) const {
                    return lhs == rhs.m_address;
                }
            };
            
            /**
             * The reserved size in bytes
            */
            std::uint32_t size() const {
                return m_stride * m_fill_map.size();
            }

            /**
             * Get span of the allocated addresses (as number of bytes)
            */
            std::uint32_t span() const;

            /**
             * The reserved number of units
            */
            std::uint32_t getUnitCount() const {
                return m_fill_map.size();
            }

            bool isFull() const;

            /**
             * Allocate a single unit
             * 
             * @return the address of the allocated unit
            */
            std::uint32_t allocUnit();
            
            /**
             * Try bounded unit allocation
            */
            std::optional<std::uint32_t> tryAllocUnit(std::optional<std::uint32_t> addr_bound);
            
            std::optional<std::uint32_t> tryAllocUnit(std::optional<std::uint32_t> addr_bound, unsigned int end_index, 
                unsigned int &hint_index);
            
            std::pair<std::uint32_t, std::uint32_t> getHint() const {
                return m_fill_map.getHint();
            }

            /**
             * Free a single unit and return true if the allocation is NOT empty
            */
            bool deallocUnit(std::uint32_t address);

            /**
             * Validate address and retrieve alloc size (i.e. stride)
            */
            std::uint32_t getAllocSize(std::uint32_t address) const;

            Stripe toStripe() const;

            /**
             * Try reducing this alloc to reclaim at least min_size of space
             * 
             * @param min_size the minimum size to reclaim
             * @return the blank corresponding to reclaimed space or an empty blank if no space was reclaimed 
            */
            Blank reclaimSpace(std::uint32_t min_size);
            
            /**
             * Calculate the address past the last allocated unit
            */
            std::uint32_t endAddr() const;
        };
        
        struct Blank
        {
            std::uint32_t m_size;
            std::uint32_t m_address;

            Blank() = default;
            Blank(std::uint32_t size, std::uint32_t address);

            // Get first aligned address within the blank (must satisfy aligned size > 0)
            std::uint32_t getAlignedAddress(std::uint32_t mask, std::uint32_t page_size) const;
            
            // Get size of the aligned block within the blank
            std::uint32_t getAlignedSize(std::uint32_t mask, std::uint32_t page_size) const;
            
            struct CompT
            {
                inline bool operator()(const Blank &lhs, const Blank &rhs) const {
                    if (lhs.m_size < rhs.m_size) {
                        return true;
                    } else if (lhs.m_size > rhs.m_size) {
                        return false;
                    } else {
                        return lhs.m_address < rhs.m_address;
                    }
                }                
            };

            // Comparator by aligned blank size
            struct AlignedCompT
            {
                const std::uint32_t m_mask;
                const std::uint32_t m_page_size;

                AlignedCompT(std::uint32_t page_size)
                    : m_mask(getPageMask(page_size))
                    , m_page_size(page_size)
                {                    
                }

                inline bool operator()(const Blank &lhs, const Blank &rhs) const
                {
                    if (lhs.getAlignedSize(m_mask, m_page_size) < rhs.getAlignedSize(m_mask, m_page_size)) {
                        return true;
                    } else if (lhs.getAlignedSize(m_mask, m_page_size) > rhs.getAlignedSize(m_mask, m_page_size)) {
                        return false;
                    } else {
                        return lhs.m_address < rhs.m_address;
                    }
                }
            };

            struct EqualT
            {
                inline bool operator()(const Blank &lhs, const Blank &rhs) const {
                    // address is unique, it's sufficient to compare by the address only
                    return lhs.m_address == rhs.m_address;
                }
            };
        };
        
        struct Stripe
        {
            std::uint32_t m_stride;
            std::uint32_t m_address;

            Stripe(std::uint32_t stride, std::uint32_t address);

            // Note that the Stripe comparison used both fields: size & address
            // but also by-size only comparison and equality check are allowed
            struct CompT
            {
                inline bool operator()(const Stripe &lhs, const Stripe &rhs) const {
                    if (lhs.m_stride < rhs.m_stride) {
                        return true;
                    } else if (lhs.m_stride > rhs.m_stride) {
                        return false;
                    } else {
                        return lhs.m_address < rhs.m_address;
                    }
                }
                
                inline bool operator()(const Stripe &lhs, std::uint32_t rhs) const {
                    return lhs.m_stride < rhs;
                }

                inline bool operator()(std::uint32_t lhs, const Stripe &rhs) const {
                    return lhs < rhs.m_stride;
                }
            };
            
            struct EqualT
            {
                inline bool operator()(const Stripe &lhs, const Stripe &rhs) const {
                    return lhs.m_stride == rhs.m_stride && lhs.m_address == rhs.m_address;
                }             

                inline bool operator()(const Stripe &lhs, std::uint32_t rhs) const {
                    return lhs.m_stride == rhs;
                }                

                inline bool operator()(std::uint32_t lhs, const Stripe &rhs) const {
                    return lhs == rhs.m_stride;
                }
            };
        };

        using AllocSetT = db0::SGB_Tree<Alloc, Alloc::CompT, Alloc::EqualT, std::uint16_t, std::uint32_t>;
        using BlankSetT = db0::SGB_Tree<Blank, Blank::CompT, Blank::EqualT, std::uint16_t, std::uint32_t>;
        using AlignedBlankSetT = db0::SGB_Tree<Blank, Blank::AlignedCompT, Blank::EqualT, std::uint16_t, std::uint32_t>;
        using StripeSetT = db0::SGB_Tree<Stripe, Stripe::CompT, Stripe::EqualT, std::uint16_t, std::uint32_t>;

    public:
        /**
         * Construct the allocator from initialized containers
         * @param page_size required for page-aligned allocations
        */
        CRDT_Allocator(AllocSetT &allocs, BlankSetT &blanks, AlignedBlankSetT &aligned_blanks, StripeSetT &stripes,
            std::uint32_t size, std::uint32_t page_size, std::optional<std::uint32_t> min_aligned_alloc_size = {});
        ~CRDT_Allocator();

        /**
         * @param align if true, the allocation will be aligned to the page boundary
         */
        std::uint64_t alloc(std::size_t size, bool aligned = false);
        
        /**
         * @param align if true, the allocation will be aligned to the page boundary
         */
        std::optional<std::uint64_t> tryAlloc(std::size_t size, bool aligned = false);
        
        void free(std::uint64_t address);
        
        std::size_t getAllocSize(std::uint64_t address) const;

        /**
         * This function allows checking the space available to this allocator dynamically
         * The bounds_fn function is called to assure the allocated address is within the currently available bounds
         * this functionality is helpful when integrating with other data structures sharing the high-rages of the same address space
         * Note that checks are performed only during the allocations / not when setting the funtion
         * 
         * @param bounds_fn the size bounds checking function
        */
        void setDynamicBound(std::function<std::uint32_t()> bounds_fn);
        
        inline std::uint32_t getMaxAddr() const {
            return m_max_addr;
        }
                
        /**
         * Get cummulative size of allocations / dellocations performed since creation of this instance
        */
        std::int64_t getAllocDelta() const {
            return m_alloc_delta;
        }

        /**
         * Get address of the 1st allocation
        */
        static std::uint64_t getFirstAddress();

        void commit();

        void detach() const;

        // Static versions of methods required for SlabAllocator integration
        // min_aligned_alloc_size must be identical as in the CRDT_Allocator constructor
        static void insertBlank(BlankSetT &blanks, AlignedBlankSetT &aligned_blanks, const Blank &blank, std::uint32_t page_size,
            std::optional<std::uint32_t> min_aligned_alloc_size = {});
        static bool isAligned(const Blank &blank, std::uint32_t page_size, std::optional<std::uint32_t> min_aligned_alloc_size = {});
        
    private:
        AllocSetT &m_allocs;
        BlankSetT &m_blanks;
        // additional index which organizes blanks (only where 0 < aligned size < 2DP)        
        AlignedBlankSetT &m_aligned_blanks;
        StripeSetT &m_stripes;
        // size of the space available to the allocator (i.e. a single slab)
        std::uint32_t m_size;
        const std::uint32_t m_page_size;
        const std::uint32_t m_min_aligned_alloc_size;
        const std::uint32_t m_shift;
        const std::uint32_t m_mask;
        std::function<std::uint32_t()> m_bounds_fn;
        // the highest allocated address
        std::uint32_t m_max_addr = 0;
        // the cummulative size of performed allocations / deallocations since creation of this instance
        // can be a negative number
        std::int64_t m_alloc_delta = 0;
        // the purpose of this cache is to speed up allocations of identical size sequences
        mutable std::unique_ptr<L0_Cache<crdt::L0_CACHE_SIZE> > m_cache;
        
        std::optional<std::uint64_t> tryAlignedAlloc(std::size_t size);

        std::optional<std::uint32_t> tryAllocFromStripe(std::uint32_t size, std::uint32_t &last_stripe_units);

        std::optional<std::uint32_t> tryAllocFromStripe(typename StripeSetT::ConstItemIterator &, std::uint32_t &last_stripe_units,
            std::optional<std::uint32_t> &cache);
        
        /**
         * Try making an allocation from registered blanks
         * 
         * @param stride the stride size (in bytes)
         * @param count the number of strides to pre-allocate         
         * @return the address of the allocated unit or std::nullopt if unable to allocate
        */
        std::optional<std::uint32_t> tryAllocFromBlanks(std::uint32_t stride, std::uint32_t count);        
        std::optional<std::uint32_t> tryAlignedAllocFromBlanks(std::uint32_t size);
        
        /**
         * Try reclaiming at least min_size bytes from registered stripes
         * 
         * @return true if space was reclaimed
        */
        bool tryReclaimSpaceFromStripes(std::uint32_t min_size);

        /**
         * Check if this blank is within current dynamic bounds (if set)
        */
        template <typename IndexT> bool inBounds(const IndexT &, const Blank &, std::uint32_t min_size, 
            std::optional<std::uint32_t> &cache) const;

        // Get first accessible address from the blank from a given index
        // the purpose of this function is to be able to retrieve aligned addresses if blank comes from the aligned index
        template <typename IndexT> std::uint32_t getFirstAddress(const IndexT &, const Blank &) const;

        /**
         * Check if the unit taken from this allocation would be within current dynamic bounds (if set)
        */
        bool inBounds(const Alloc &, std::optional<std::uint32_t> &cache) const;

        // Erase either from blanks or from aligned blanks
        template <typename IndexT> void erase(IndexT &index, const Blank &);

        // Find blank of a given minimal size and remove it from a specific index
        template <typename IndexT> std::optional<Blank> tryPullBlank(IndexT &index, std::uint32_t min_size);

        // Erase record from the corresponding indexes (m_blanks / m_aligned_blanks)
        void erase(const Blank &);        
        // Insert with proper indexes (m_blanks / m_aligned_blanks)
        void insertBlank(const Blank &);
        
        // Checks if the aligned size is in the range 0 < aligned size < 1DP
        bool isAligned(const Blank &) const;
    };
    
    template <typename IndexT> bool CRDT_Allocator::inBounds(const IndexT &index, const Blank &blank, std::uint32_t size,
        std::optional<std::uint32_t> &cache) const
    {
        // this is to avoid unnecessary calls to bounds_fn
        if (!cache && m_bounds_fn) {
            cache = m_bounds_fn();
        }
        if (cache) {
            return getFirstAddress(index, blank) + size <= *cache;
        }
        return true;
    }

    template <typename IndexT>
    void CRDT_Allocator::erase(IndexT &index, const Blank &blank)
    {
        auto it = index.find_equal(blank);
        if (!it.first) {
            THROWF(db0::InternalException) << "CRDT_Allocator internal error: blank not found";
        }
        index.erase(it);
    }
    
    template <typename IndexT>
    std::optional<CRDT_Allocator::Blank> CRDT_Allocator::tryPullBlank(IndexT &index, std::uint32_t min_size)
    {
        // note that for aligned blanks the size will represent the aligned size
        auto blank_ptr = index.upper_equal_bound(Blank(min_size, 0));
        // no blank of sufficient size (or aligned size) exists
        if (!blank_ptr) {
            return std::nullopt;
        }
        
        // validate dynamic bounds if such exist
        Blank blank;
        std::optional<std::uint32_t> cache;
        if (inBounds(index, *blank_ptr.first, min_size, cache)) {
            blank = *blank_ptr.first;
        } else {
            // try with other registered blanks (possibly bigger size)
            auto it = index.upper_slice(blank_ptr);
            ++it;
            while (!it.is_end()) {
                if ((*it).m_size >= min_size && inBounds(index, *it, min_size, cache)) {
                    break;
                }
                ++it;
            }
            // no blanks of sufficient size / within bounds exist
            if (it.is_end()) {
                return std::nullopt;
            }
            blank = *it;
        }
        
        // remove the blank
        index.erase(blank_ptr);
        if ((void*)&index == (void*)&m_blanks) {
            // need also to remove from aligned blanks (if aligned)
            if (isAligned(blank)) {
                erase(m_aligned_blanks, blank);
            }
        } else {
            assert((void*)&index == (void*)&m_aligned_blanks);
            // need also to remove from regular blanks (always present)
            erase(m_blanks, blank);
        }
        
        return blank;
    }
    
    template <typename IndexT>
    std::uint32_t CRDT_Allocator::getFirstAddress(const IndexT &index, const Blank &blank) const
    {
        if ((void*)&index == (void*)&m_aligned_blanks) {
            return blank.getAlignedAddress(m_mask, m_page_size);
        }  
        assert((void*)&index == (void*)&m_blanks);
        return blank.m_address;
    }

}

namespace std 

{

    ostream &operator<<(ostream &os, const db0::CRDT_Allocator::Alloc &);
    ostream &operator<<(ostream &os, const db0::CRDT_Allocator::Blank &);
    ostream &operator<<(ostream &os, const db0::CRDT_Allocator::Stripe &);

}