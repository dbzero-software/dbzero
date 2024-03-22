#include <dbzero/core/crdt/CRDT_Allocator.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <stdexcept>
#include <iostream>

namespace db0

{

    template <int N> class L0_Cache
    {
    public:
        using Alloc = CRDT_Allocator::Alloc;
        using AllocSetT = CRDT_Allocator::AllocSetT;
        using AllocIterator = AllocSetT::ConstItemIterator;

        L0_Cache(AllocSetT &allocs)
            : m_allocs(allocs)
        {
        }
        
        std::optional<std::uint32_t> tryAlloc(std::size_t size, std::optional<std::uint32_t> addr_bound)
        {
            auto hint = &m_hints[0];
            for (auto &alloc : m_cache) {
                if (alloc && alloc.first->m_stride == size) {
                    // the const_cast is safe because we store mutated items
                    auto result = const_cast<Alloc*>(alloc.first)->tryAllocUnit(addr_bound, hint->first, hint->second);
                    if (!result || alloc.first->isFull()) {
                        // invalidate cache element
                        alloc = {};
                    }
                    return result;               
                }
                ++hint;
            }
            return std::nullopt;
        }
        
        void addMutable(AllocIterator new_alloc, Alloc *) 
        {
            auto hint = &m_hints[0];
            auto new_hint = new_alloc.first->getHint();
            for (auto &alloc : m_cache) {
                if (!alloc) {
                    alloc = new_alloc;
                    *hint =  new_hint;
                    return;
                }
                std::swap(alloc, new_alloc);
                std::swap(*hint, new_hint);
                ++hint;
            }
        }

        /**
         * Invalidate all cached iterators
        */
        void clear() {
            m_cache = {};
        }

    private:
        AllocSetT &m_allocs;
        std::array<AllocIterator, N> m_cache;
        std::array<std::pair<std::uint32_t, std::uint32_t>, N> m_hints;
    };
    
    CRDT_Allocator::CRDT_Allocator(AllocSetT &allocs, BlankSetT &blanks, StripeSetT &stripes, std::uint32_t size)
        : m_allocs(allocs)
        , m_blanks(blanks)
        , m_stripes(stripes)
        , m_size(size)
        , m_cache(std::make_unique<L0_Cache<crdt::L0_CACHE_SIZE> >(m_allocs))
    {
        if (!m_allocs.empty()) {
            m_max_addr = m_allocs.find_max().first->endAddr();
        }
    }

    CRDT_Allocator::~CRDT_Allocator()
    {
    }

    CRDT_Allocator::Alloc::Alloc(std::uint32_t address, std::uint32_t stride, std::uint32_t size)
        : m_address(address)
        , m_stride(stride)
        , m_fill_map(size)
    {
        assert(m_stride > 0);
    }

    std::uint32_t CRDT_Allocator::Alloc::allocUnit()
    {
        return m_address + m_stride * m_fill_map.allocUnit();
    }

    std::optional<std::uint32_t> CRDT_Allocator::Alloc::tryAllocUnit(std::optional<std::uint32_t> addr_bound)
    {
        auto revert = m_fill_map;
        auto result = m_address + m_stride * m_fill_map.allocUnit();
        // would fall out of bound, revert
        if (addr_bound && result + m_stride > *addr_bound) {
            m_fill_map = revert;
            return std::nullopt;
        }
        return result;
    }

    std::optional<std::uint32_t> CRDT_Allocator::Alloc::tryAllocUnit(std::optional<std::uint32_t> addr_bound, unsigned int end_index,
        unsigned int &hint_index)
    {
        auto revert = m_fill_map;
        auto result = m_address + m_stride * m_fill_map.allocUnit(end_index, hint_index);
        // would fall out of bound
        if (addr_bound && result + m_stride > *addr_bound) {
            m_fill_map = revert;
            return std::nullopt;
        }
        return result;      
    }

    bool CRDT_Allocator::Alloc::isFull() const {
        return m_fill_map.all();
    }

    std::uint32_t CRDT_Allocator::Alloc::endAddr() const {
        return m_address + m_fill_map.size() * m_stride;
    }

    std::uint32_t CRDT_Allocator::Alloc::span() const {
        return m_fill_map.span() * m_stride;
    }

    std::uint32_t CRDT_Allocator::Alloc::getAllocSize(std::uint32_t address) const 
    {
        // Get allocation size under a specific address or throw exception
        if ((address >= m_address) && ((address - m_address) % m_stride == 0) && (address < m_address + m_stride * m_fill_map.size()) &&
            m_fill_map[int((address - m_address) / m_stride)]) {
            return m_stride;
        }
        THROWF(db0::InternalException) << "Invalid address: " << address << THROWF_END;
    }

    bool CRDT_Allocator::Alloc::deallocUnit(std::uint32_t address)
    {
        if ((address >= m_address) && ((address - m_address) % m_stride == 0) && (address < m_address + m_stride * m_fill_map.size())) {
            auto index = int((address - m_address) / m_stride);
            if (!m_fill_map[index]) {
                THROWF(db0::InternalException) << "Invalid address: " << address;
            }
            m_fill_map.reset(index);
        }
        return !m_fill_map.empty();
    }
    
    CRDT_Allocator::Blank CRDT_Allocator::Alloc::reclaimSpace(std::uint32_t min_size) 
    {
        auto old_size = size();
        auto unit_count = (min_size - 1) / m_stride + 1;
        auto resized = m_fill_map.tryDownsize(unit_count);
        // return the reclaimed space (size / address)
        return { resized * m_stride, m_address + old_size - resized * m_stride };
    }

    CRDT_Allocator::Stripe CRDT_Allocator::Alloc::toStripe() const {
        return Stripe(m_stride, m_address);
    }
    
    bool CRDT_Allocator::FillMap::operator[](unsigned int index) const {
        return m_data & ((crdt::bitarray_t)0x01 << index);
    }

    std::uint32_t CRDT_Allocator::FillMap::span() const {
        return size() - unused();
    }

    unsigned int CRDT_Allocator::FillMap::unused() const
    {
        unsigned int unused = 0;
        auto size_ = size();
        crdt::bitarray_t m_mask = (crdt::bitarray_t)0x01 << (size_ - 1);
        for (unsigned int i = 0;i < size_; ++i) {
            if (m_data & m_mask) {
                return unused;
            }
            ++unused;
            m_mask >>= 1;
        }
        return unused;
    }
    
    unsigned int CRDT_Allocator::FillMap::tryDownsize(unsigned int min_units) 
    {
        auto unused_units = unused();
        if (unused_units >= min_units) {
            crdt::bitarray_t size_id = m_data >> crdt::SIZE_MAP[0];
            auto new_id = size_id + 1;
            for (; new_id < crdt::NSIZE; ++new_id) {
                auto diff_units = crdt::SIZE_MAP[size_id] - crdt::SIZE_MAP[new_id];
                if (diff_units >= min_units && diff_units <= unused_units) {
                    m_data = (m_data & crdt::NSIZE_MASK()) | (new_id << crdt::SIZE_MAP[0]);
                    // diff_units reclaimed
                    return diff_units;
                }
            }
        }
        // unable to reclaim any space
        return 0;
    }
    
    std::uint32_t CRDT_Allocator::FillMap::allocUnit() 
    {
        crdt::bitarray_t m_mask = 0x01;
        std::uint32_t end_index = size();
        for (std::uint32_t index = 0;index != end_index; m_mask <<= 1, ++index) {
            if (!(m_data & m_mask)) {
                m_data |= m_mask;
                return index;
            }
        }
        THROWF(db0::InternalException) << "FillMap: allocUnit failed" << THROWF_END;
    }

    std::uint32_t CRDT_Allocator::FillMap::allocUnit(std::uint32_t end_index, std::uint32_t &hint_index)
    {        
        crdt::bitarray_t m_mask = (crdt::bitarray_t)0x01 << hint_index;
        for (;hint_index != end_index; m_mask <<= 1, ++hint_index) {
            if (!(m_data & m_mask)) {
                m_data |= m_mask;
                return hint_index++;
            }
        }
        hint_index = allocUnit();
        return hint_index++;
    }
    
    std::pair<std::uint32_t, std::uint32_t> CRDT_Allocator::FillMap::getHint() const {
        return { size(), 0 };
    }
    
    bool CRDT_Allocator::FillMap::all() const
    {
        return (m_data & crdt::NSIZE_MASK()) == crdt::m_masks[m_data >> crdt::SIZE_MAP[0]];
    }

    bool CRDT_Allocator::FillMap::empty() const
    {
        return !(m_data & crdt::NSIZE_MASK());
    }

    void CRDT_Allocator::FillMap::reset(unsigned int index) 
    {
        m_data &= ~((crdt::bitarray_t)0x01 << index);
    }
    
    CRDT_Allocator::Stripe::Stripe(std::uint32_t stride, std::uint32_t address)
        : m_stride(stride)
        , m_address(address)
    {
        assert(m_stride > 0);
    }

    CRDT_Allocator::Blank::Blank(std::uint32_t size, std::uint32_t address)
        : m_size(size)
        , m_address(address)
    {
    }
    
    std::optional<std::uint64_t> CRDT_Allocator::tryAlloc(std::size_t size)
    {
        assert(size > 0);
        std::uint32_t last_stripe_units = 0;
        auto result  = tryAllocFromStripe(size, last_stripe_units);
        if (result) {
            m_alloc_delta += size;
            return *result;
        }

        // try with decreasing stripe sizes, starting from double the size of the last one
        unsigned int start_index = crdt::NSIZE - 1;
        while (start_index > 0 && last_stripe_units >= crdt::SIZE_MAP[start_index])
            --start_index;
        
        for (;;) {
            if (!m_blanks.empty()) {
                std::optional<std::uint32_t> max_blank_size;
                for (unsigned int index = start_index; index < 4; ++index) {
                    if (max_blank_size && *max_blank_size < size * crdt::SIZE_MAP[index]) {
                        continue;
                    }
                    result = tryAllocFromBlanks(size, crdt::SIZE_MAP[index]);
                    if (result) {
                        m_alloc_delta += size;
                        return *result;
                    }
                    // get the max blank size
                    if (!max_blank_size) {
                        auto max = m_blanks.find_max();
                        assert(max.first);
                        max_blank_size = max.first->m_size;
                    }            
                }
            }
            
            if (!tryReclaimSpaceFromStripes(size)) {
                break;
            }
        }

        // out of memory
        return std::nullopt;        
    }

    bool CRDT_Allocator::tryReclaimSpaceFromStripes(std::uint32_t min_size) 
    {
        using AllocWindowT = typename CRDT_Allocator::AllocSetT::WindowT;

        // visit stripes in a semi-ordered way
        // starting from the highest nodes (the largest stride values)
        auto it = m_stripes.rbegin_unsorted();
        while (!it.is_end()) {
            const auto &stripe = *it;
            // pruning rule
            if (stripe.m_stride * (crdt::SIZE_MAP[0] - 1) < min_size) {
                // exit since likelihood of finding enough space which can be reclaimed is low
                return false;
            }
            // find the corresponding alloc (window)
            AllocWindowT alloc_window;
            if (!m_allocs.lower_equal_window(stripe.m_address, alloc_window)) {
                THROWF(db0::InternalException) << "Invalid address: " << stripe.m_address;
            }
            
            auto &alloc = *m_allocs.modify(alloc_window[1]);
            auto old_size = alloc.size();
            auto blank = alloc.reclaimSpace(min_size);
            if (blank.m_size > 0) {
                // reclaimed space may affect the max address
                m_max_addr = m_allocs.find_max().first->endAddr();
                // merge with the neighboring blank if such exists
                std::optional<Blank> b1;
                if (alloc_window[2]) {
                    // right neighbor exists
                    auto &right = *alloc_window[2].first;
                    auto b1_size = right.m_address - alloc.m_address - old_size;
                    if (b1_size > 0) {
                        b1 = Blank(b1_size, right.m_address - b1_size);
                    }
                } else {
                    if (alloc.m_address + old_size < m_size) {
                        // last allocation but there's blank space right of it
                        b1 = Blank(m_size - alloc.m_address - old_size, alloc.m_address + old_size);
                    }
                }

                if (b1) {
                    auto it = m_blanks.find_equal(*b1);
                    if (!it.first) {
                        THROWF(db0::InternalException) << "CRDT_Allocator internal error: blank not found";
                    }
                    m_blanks.erase(it);
                    blank.m_size += b1->m_size;
                }

                // space has been successfully reclaimed, now add the newly created blank
                m_blanks.insert(blank);
                // remove the stripe if this alloc is full
                if (alloc.isFull()) {
                    m_stripes.erase(it);
                }
                return true;
            }
            ++it;
        }
        return false;
    }

    void CRDT_Allocator::free(std::uint64_t address)
    {
        using AllocWindowT = typename CRDT_Allocator::AllocSetT::WindowT;
        AllocWindowT alloc_window;
        if (!m_allocs.lower_equal_window(address, alloc_window)) {
            THROWF(db0::InternalException) << "Invalid address: " << address;            
        }

        assert(alloc_window[1]);
        const auto alloc = *alloc_window[1].first;
        m_alloc_delta -= alloc.m_stride;
        auto was_full = alloc.isFull();
        // modify the central item (i.e. alloc_window[1])
        if (m_allocs.modify(alloc_window)->deallocUnit(address)) {
            if (was_full) {
                // add stripe if it does not exist
                auto stripe = m_stripes.find_equal(alloc.toStripe());
                if (!stripe.first) {
                    m_stripes.insert(alloc.toStripe());
                }
            }
            // just deallocated a single unit
            return;
        }

        // if the associated stripe exists then remove it
        {
            auto stripe = m_stripes.find_equal(alloc.toStripe());
            if (stripe.first) {
                m_stripes.erase(stripe);
            }
        }

        // we need to remove the alloc entry since it's empty
        std::optional<Blank> b0, b1;
        if (alloc_window[0]) {
            // left neighbor exists
            auto &left = *alloc_window[0].first;
            auto b0_size = alloc.m_address - left.m_address - left.size();
            if (b0_size > 0) {
                b0 = Blank(b0_size, left.m_address + left.size());
            }
        } else {
            if (alloc.m_address > 0) {
                b0 = Blank(alloc.m_address, 0);
            }
        }

        if (alloc_window[2]) {
            // right neighbor exists
            auto &right = *alloc_window[2].first;
            auto b1_size = right.m_address - alloc.m_address - alloc.size();
            if (b1_size > 0) {
                b1 = Blank(b1_size, right.m_address - b1_size);
            }
        } else {
            if (alloc.m_address + alloc.size() < m_size) {
                // last allocation but there's blank space right of it
                b1 = Blank(m_size - alloc.m_address - alloc.size(), alloc.m_address + alloc.size());
            }
        }

        // remove blanks
        if (b0) {
            auto it = m_blanks.find_equal(*b0);
            if (!it.first) {
               THROWF(db0::InternalException) << "CRDT_Allocator internal error. Blank not found: " << *b0;
            }
            m_blanks.erase(it);
        }
        if (b1) {
            auto it = m_blanks.find_equal(*b1);
            if (!it.first) {
               THROWF(db0::InternalException) << "CRDT_Allocator internal error. Blank not found: " << *b1;
            }
            m_blanks.erase(it);
        }
        
        // L0 cache must be invalidated
        m_cache->clear();
        // remove the allocation
        m_allocs.erase(alloc_window[1]);
        if (m_allocs.empty()) {
            m_max_addr = 0;
        } else {
            m_max_addr = m_allocs.find_max().first->endAddr();
        }
        // re-insert the merged blank
        if (!b0) {
            b0 = Blank(alloc.size(), alloc.m_address);
        }
        if (!b1) {
            b1 = Blank(alloc.size(), alloc.m_address);
        }

        m_blanks.insert(Blank(b1->m_address + b1->m_size - b0->m_address, b0->m_address));
    }
    
    std::size_t CRDT_Allocator::getAllocSize(std::uint64_t address) const
    {
        auto alloc = m_allocs.lower_equal_bound(address);
        if (!alloc) {
            THROWF(db0::InternalException) << "Invalid address: " << address;            
        }
        return alloc.first->getAllocSize(address);
    }

    std::optional<std::uint32_t> CRDT_Allocator::tryAllocFromBlanks(std::uint32_t stride, std::uint32_t count)
    {
        // find the 1st blank of sufficient size (i.e. >= stride * count)
        auto min_size = stride * count;
        auto blank_ptr = m_blanks.upper_equal_bound(Blank(min_size, 0));
        if (!blank_ptr) {
            return std::nullopt;
        }
        
        // validate dynamic bounds if such exist
        Blank blank;
        std::optional<std::uint32_t> cache;
        if (inBounds(*blank_ptr.first, min_size, cache)) {
            blank = *blank_ptr.first;
        } else {
            // try with other registered blanks (possibly bigger size)
            auto it = m_blanks.upper_slice(blank_ptr);
            ++it;
            while (!it.is_end()) {
                if ((*it).m_size >= min_size && inBounds(*it, min_size, cache)) {
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
        m_blanks.erase(blank_ptr);
        // L0 cache must be invalidated
        m_cache->clear();
        auto alloc = m_allocs.emplace(blank.m_address, stride, count);
        m_max_addr = std::max(m_max_addr, alloc.first->endAddr());        
        auto result = alloc.first->allocUnit();
        if (count > 1) {
            // register with L0 cache
            m_cache->addMutable(alloc, alloc.first);
        }
        // register the new alloc with stripes (even if count == 1)        
        m_stripes.insert(alloc.first->toStripe());

        if (blank.m_size > min_size) {
            // register remaining part of the blank
            // note that the remaining part is registered even if it falls outside of the dynamic bounds
            // this is by design since the dynamic bounds may change in the future
            m_blanks.insert(Blank(blank.m_size - stride * count, blank.m_address + stride * count));
        }
        return result;
    }
    
    std::optional<std::uint32_t> CRDT_Allocator::tryAllocFromStripe(typename StripeSetT::ConstItemIterator &stripe,
        std::uint32_t &last_stripe_units, std::optional<std::uint32_t> &addr_bound)
    {
        // find the corresponding alloc next
        auto alloc = m_allocs.find_equal(stripe.first->m_address);
        if (!alloc.first) {
            THROWF(db0::InternalException) << "CRDT_Allocator internal error: alloc not found";
        }

        if (alloc.first->isFull()) {
            last_stripe_units = alloc.first->getUnitCount();
            // remove from stripes
            assert(stripe.validate());
            m_stripes.erase(stripe);
            return std::nullopt;
        }

        // allocate from existing stripe
        auto alloc_ptr = m_allocs.modify(alloc);
        auto result = alloc_ptr->tryAllocUnit(addr_bound);
        if (result && !alloc_ptr->isFull()) {
            // register with cache for fast future retrieval
            m_cache->addMutable(alloc, alloc_ptr);
        }

        return result;
    }
    
    std::optional<std::uint32_t> CRDT_Allocator::tryAllocFromStripe(std::uint32_t size, std::uint32_t &last_stripe_units)
    {
        std::optional<std::uint32_t> addr_bound;
        if (m_bounds_fn) {
            addr_bound = m_bounds_fn();
        }
        
        auto result = m_cache->tryAlloc(size, addr_bound);
        if (result) {
            return result;
        }
        
        // Find stripe of exactly matching size
        last_stripe_units = 0;
        auto stripe_ptr = m_stripes.lower_equal_bound(size);
        assert(stripe_ptr.validate());
        if (!stripe_ptr || stripe_ptr.first->m_stride != size) {
            return std::nullopt;
        }
        
        result = tryAllocFromStripe(stripe_ptr, last_stripe_units, addr_bound);
        if (last_stripe_units > 0 || result) {
            return result;
        }
        
        // try with other registered stripes (which might be within the dynamic bounds)
        auto it = m_stripes.upper_slice(stripe_ptr);
        auto node = it.get().second;
        ++it;
        while (!it.is_end()) {
            if ((*it).m_stride == size) {
                result = tryAllocFromStripe(it.getMutable(), last_stripe_units, addr_bound);
                if (last_stripe_units > 0 || result) {
                    return result;
                }
            }
            if ((*it).m_stride > size && it.get().second != node) {
                // pruning rule, no more stripes potentially matching the size exist
                break;
            }
            ++it;
        }
        return result;
    }
    
    std::uint64_t CRDT_Allocator::alloc(std::size_t size) 
    {
        auto result = tryAlloc(size);
        if (!result) {
            THROWF(InternalException) << "CRDT_Allocator: out of memory" << THROWF_END;
        }
        return *result;
    }

    void CRDT_Allocator::setDynamicBound(std::function<std::uint32_t()> bounds_fn) {
        this->m_bounds_fn = bounds_fn;
    }

    bool CRDT_Allocator::inBounds(const Blank &blank, std::uint32_t min_size, std::optional<std::uint32_t> &cache) const 
    {
        // this is to avoid unnecessary calls to bounds_fn
        if (!cache && m_bounds_fn) {
            cache = m_bounds_fn();
        }
        if (cache) {
            return blank.m_address + min_size <= *cache;
        }
        return true;
    }

    std::uint64_t CRDT_Allocator::getFirstAddress() {
        return 0;
    }
    
    void CRDT_Allocator::commit()
    {        
        m_cache->clear();
    }

}

namespace std

{

    ostream &operator<<(ostream &os, const db0::CRDT_Allocator::Blank &blank) {
        return os << "size=" << blank.m_size << ", address=" << blank.m_address;        
    }
    
    ostream &operator<<(ostream &os, const db0::CRDT_Allocator::Alloc &alloc) {
        return os << "address=" << alloc.m_address << ", stride=" << alloc.m_stride << ", size=" << alloc.size();
    }

    ostream &operator<<(ostream &os, const db0::CRDT_Allocator::Stripe &stripe) {
        return os << "stride=" << stripe.m_stride << ", address=" << stripe.m_address;
    }
    
}
