// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <stdexcept>
#include <iostream>
#include <dbzero/core/crdt/CRDT_Allocator.hpp>
#include <dbzero/core/exception/Exceptions.hpp>

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
                if (!alloc.isEnd() && alloc.first->m_stride == size) {
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
                if (alloc.isEnd()) {
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
    
    std::uint32_t getMinAlignedAllocSize(std::optional<std::uint32_t> min_aligned_alloc_size, std::uint32_t page_size)
    {
        // use page_size + 1 as the default minimum
        if (!min_aligned_alloc_size) {
            return page_size + 1;
        }
        return *min_aligned_alloc_size;
    }

    CRDT_Allocator::CRDT_Allocator(AllocSetT &allocs, BlankSetT &blanks, AlignedBlankSetT &aligned_blanks, StripeSetT &stripes,
        std::uint32_t size, std::uint32_t page_size, std::optional<std::uint32_t> min_aligned_alloc_size)
        : m_allocs(allocs)
        , m_blanks(blanks)
        , m_aligned_blanks(aligned_blanks)
        , m_stripes(stripes)
        , m_size(size)
        , m_page_size(page_size)
        , m_critical_margin(page_size * 2)
        , m_min_aligned_alloc_size(getMinAlignedAllocSize(min_aligned_alloc_size, page_size))
        , m_shift(db0::getPageShift(page_size))
        , m_mask(getPageMask(page_size))
        , m_cache(std::make_unique<L0_Cache<crdt::L0_CACHE_SIZE> >(m_allocs))
    {
        assert(!m_bounds_fn || std::get<0>(m_bounds_fn()) < std::get<1>(m_bounds_fn()));
        assert(!m_bounds_fn || std::get<1>(m_bounds_fn()) < std::get<2>(m_bounds_fn()));
        if (!m_allocs.empty()) {
            m_max_addr = m_allocs.find_max().first->endAddr();
        }        
        assert(!m_bounds_fn || m_max_addr <= std::get<2>(m_bounds_fn()));
    }

    CRDT_Allocator::~CRDT_Allocator()
    {
    }
    
    CRDT_Allocator::Alloc::Alloc(std::uint32_t address, std::uint32_t stride, std::uint32_t size, bool has_stripe)
        : m_address(address)
        , m_stride(stride)
        , m_fill_map(size, has_stripe)
    {
        assert(m_stride > 0);
    }

    std::uint32_t CRDT_Allocator::Alloc::allocUnit() {
        return m_address + m_stride * m_fill_map.allocUnit();
    }
    
    void CRDT_Allocator::Alloc::setHasStripe(bool has_stripe) {
        m_fill_map.setHasStripe(has_stripe);
    }
    
    void CRDT_Allocator::Alloc::setLostStripe() {
        m_fill_map.setLostStripe();
    }

    std::uint32_t CRDT_Allocator::Alloc::capacity() const {
        return m_fill_map.size() * m_stride;
    }

    std::optional<std::uint32_t> CRDT_Allocator::Alloc::tryAllocUnit(std::optional<std::uint32_t> addr_bound)
    {
        if (isFull()) {
            return std::nullopt;
        }

        auto revert = m_fill_map;
        auto result = m_address + m_stride * m_fill_map.allocUnit();
        // would fall out of bound, revert
        if (addr_bound && result + m_stride > *addr_bound) {
            m_fill_map = revert;
            return std::nullopt;
        }
        return result;
    }

    std::optional<std::uint32_t> CRDT_Allocator::Alloc::tryAllocUnit(
        std::optional<std::uint32_t> addr_bound, unsigned int end_index, unsigned int &hint_index)
    {
        if (isFull()) {
            return std::nullopt;
        }
        
        auto revert = m_fill_map;
        auto hint_revert = hint_index;
        auto result = m_address + m_stride * m_fill_map.allocUnit(end_index, hint_index);
        // would fall out of bound
        if (addr_bound && result + m_stride > *addr_bound) {
            m_fill_map = revert;
            hint_index = hint_revert;
            return std::nullopt;
        }
        return result;   
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
        if (!isAllocated(address)) {
            THROWF(db0::BadAddressException) << "Invalid address: " << address << THROWF_END;
        }
        return m_stride;
    }
    
    bool CRDT_Allocator::Alloc::isAllocated(std::uint32_t address, std::size_t *size_of_result) const
    {
        if (((address >= m_address) && ((address - m_address) % m_stride == 0) && (address < m_address + m_stride * m_fill_map.size()) &&
            m_fill_map[int((address - m_address) / m_stride)]))
        {
            if (size_of_result) {
                *size_of_result = m_stride;
            }
            return true;
        }
        return false;
    }

    bool CRDT_Allocator::Alloc::deallocUnit(std::uint32_t address)
    {
        if ((address >= m_address) && ((address - m_address) % m_stride == 0) && (address < m_address + m_stride * m_fill_map.size())) {
            auto index = int((address - m_address) / m_stride);
            if (!m_fill_map[index]) {
                THROWF(db0::BadAddressException) << "Invalid address: " << address;
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

    bool CRDT_Allocator::Alloc::canReclaimSpace(std::uint32_t min_size) const
    {
        auto unit_count = (min_size - 1) / m_stride + 1;
        return m_fill_map.canDownsize(unit_count);
    }

    CRDT_Allocator::Stripe CRDT_Allocator::Alloc::toStripe() const {
        return Stripe(m_stride, m_address);
    }
    
    CRDT_Allocator::FillMap::FillMap(std::uint32_t size, bool has_stripe)
        : m_data(has_stripe ? db0::crdt::HAS_STRIPE_BIT : 0)
    {
        if (size == crdt::SIZE_MAP[3]) {
            m_data |= (crdt::bitarray_t)0x3 << crdt::SIZE_MAP[0];
        } else if (size == crdt::SIZE_MAP[2]) {
            m_data |= (crdt::bitarray_t)0x2 << crdt::SIZE_MAP[0];
        } else if (size == crdt::SIZE_MAP[1]) {
            m_data |= (crdt::bitarray_t)0x1 << crdt::SIZE_MAP[0];
        } else if (size == crdt::SIZE_MAP[0]) {
            m_data |= 0x0;
        } else {
            THROWF(InternalException) << "invalid size (FillMap): " << size << THROWF_END;
        }
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
            crdt::bitarray_t size_id = this->sizeId();
            auto new_id = size_id + 1;
            for (; new_id < crdt::NSIZE; ++new_id) {
                assert(crdt::SIZE_MAP[size_id] > crdt::SIZE_MAP[new_id]);
                // reclaim space if the difference is within the unused units
                auto diff_units = crdt::SIZE_MAP[size_id] - crdt::SIZE_MAP[new_id];
                if (diff_units >= min_units && diff_units <= unused_units) {
                    // assign new size-id without affecting other reserved bits
                    m_data = (m_data & crdt::NSIZE_MASK()) | (new_id << crdt::SIZE_MAP[0]);
                    // diff_units reclaimed
                    return diff_units;
                }
            }
        }
        // unable to reclaim any space
        return 0;
    }
    
    bool CRDT_Allocator::FillMap::canDownsize(unsigned int min_units) const
    {
        auto unused_units = unused();
        if (unused_units >= min_units) {
            crdt::bitarray_t size_id = this->sizeId();
            auto new_id = size_id + 1;
            for (; new_id < crdt::NSIZE; ++new_id) {
                assert(crdt::SIZE_MAP[size_id] > crdt::SIZE_MAP[new_id]);
                auto diff_units = crdt::SIZE_MAP[size_id] - crdt::SIZE_MAP[new_id];
                if (diff_units >= min_units && diff_units <= unused_units) {
                    return true;
                }
            }
        }
        // unable to reclaim any space
        return false;
    }
    
    void CRDT_Allocator::FillMap::setLostStripe() {
        m_data |= crdt::LOST_STRIPE_BIT;        
    }
    
    void CRDT_Allocator::FillMap::setHasStripe(bool has_stripe)
    {
        if (has_stripe) {
            m_data &= ~crdt::LOST_STRIPE_BIT; // clear the lost-stripe bit
            m_data |= crdt::HAS_STRIPE_BIT;
        } else {
            m_data &= ~crdt::HAS_STRIPE_BIT;
        }
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
        assert(false);
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
    
    bool CRDT_Allocator::FillMap::empty() const {
        return !(m_data & crdt::NRESERVED_MASK());
    }

    void CRDT_Allocator::FillMap::reset(unsigned int index) {
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

    std::optional<std::uint64_t> CRDT_Allocator::tryAlignedAlloc(std::size_t size)
    {
        assert(size >= m_min_aligned_alloc_size);
        // unable to alloc from blanks when no in the "green zone"
        if (!greenZone()) {
            return std::nullopt;
        }

        for (;;) {
            if (!m_blanks.empty() || !m_aligned_blanks.empty()) {
                auto result = tryAlignedAllocFromBlanks(size);
                if (result) {
                    // address must be within the dynamic bounds (below red limit)
                    assert(!redZone());
                    m_alloc_delta += size;
                    return *result;
                }
            }
            
            // try raclaiming aligned space (at least size + 1DP - 1)
            if (!tryReclaimSpaceFromStripes(std::max(size, static_cast<std::size_t>(size * m_page_size - 1)))) {
                break;
            }
        }
        
        // out of memory
        return std::nullopt;
    }

    std::optional<std::uint64_t> CRDT_Allocator::tryAlloc(std::size_t size, bool align)
    {
        assert(size > 0);
        // page-aligned allocs have a special handling
        if (align) {
            return tryAlignedAlloc(size);
        }

        std::uint32_t last_stripe_units = 0;
        // aligned ranges cannot be allocated from stripes        
        auto result = tryAllocFromStripe(size, last_stripe_units);
        if (result) {
            // address must be within the dynamic bounds (below red limit)
            assert(!redZone());
            m_alloc_delta += size;
            return *result;
        }

        // try alloc from blanks only when in the "green zone"
        if (!greenZone()) {
            return std::nullopt;
        }

        // try with decreasing stripe sizes, starting from double the size of the last one
        unsigned int start_index = crdt::NSIZE - 1;
        while (start_index > 0 && last_stripe_units >= crdt::SIZE_MAP[start_index])
            --start_index;
        
        for (;;) {
            if (!m_blanks.empty() || !m_aligned_blanks.empty()) {
                std::optional<std::uint32_t> max_blank_size;
                for (unsigned int index = start_index; index < 4; ++index) {
                    if (max_blank_size && *max_blank_size < size * crdt::SIZE_MAP[index]) {
                        continue;
                    }
                    result = tryAllocFromBlanks(size, crdt::SIZE_MAP[index]);
                    if (result) {
                        // address must be within the dynamic bounds (below red limit)
                        assert(!redZone());
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
    
    void CRDT_Allocator::eraseBlank(const Blank &blank)
    {
        // primary index, holds all blanks
        eraseBlank(m_blanks, blank);
        if (isAligned(blank)) {
            // secondary index, holds aligned blanks only
            eraseBlank(m_aligned_blanks, blank);
        }
    }
    
    void CRDT_Allocator::insertBlank(const Blank &blank)
    {
        m_blanks.insert(blank);
        if (isAligned(blank)) {
            m_aligned_blanks.insert(blank);
        }
    }
    
    void CRDT_Allocator::insertBlank(BlankSetT &blanks, AlignedBlankSetT &aligned_blanks, const Blank &blank,
        std::uint32_t page_size, std::optional<std::uint32_t> min_aligned_alloc_size)
    {
        blanks.insert(blank);
        if (isAligned(blank, page_size, min_aligned_alloc_size)) {
            aligned_blanks.insert(blank);
        }
    }
    
    bool CRDT_Allocator::tryReclaimSpaceFromStripes(std::uint32_t min_size)
    {
        using AllocWindowT = typename CRDT_Allocator::AllocSetT::WindowT;
        
        if (!greenZone()) {
            // operation disallowed when not in the "green zone"
            return false;
        }

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
                THROWF(db0::BadAddressException) << "Invalid address: " << stripe.m_address;
            }
            
            // NOTE: modify invalidates the entire window, therefore dedicated "modify" version is used
            assert(!alloc_window[1].isEnd());
            // the additional check is to avoid unnecessary modifications
            if (alloc_window[1].first->canReclaimSpace(min_size)) {
                auto &alloc = *m_allocs.modify(alloc_window);
                auto old_size = alloc.size();
                auto blank = alloc.reclaimSpace(min_size);
                if (blank.m_size > 0) {
                    assert(blank.m_size >= min_size);
                    assert(alloc.size() == old_size - blank.m_size);
                    // reclaimed space may affect the max address (since it's stripe related)
                    m_max_addr = m_allocs.find_max().first->endAddr();
                    assert(!redZone());
                    // merge with the neighboring blank if such exists
                    std::optional<Blank> b1;
                    if (!alloc_window[2].isEnd()) {
                        // right neighbor exists
                        auto &right = *alloc_window[2].first;
                        auto b1_size = right.m_address - alloc.m_address - old_size;
                        if (b1_size > 0) {
                            b1 = Blank(b1_size, right.m_address - b1_size);
                        }
                    } else {
                        if (alloc.m_address + old_size < m_size) {
                            // last allocation but there's blank space right of it
                            // may be either regular or aligned
                            b1 = Blank(m_size - alloc.m_address - old_size, alloc.m_address + old_size);
                        }
                    }

                    if (b1) {
                        eraseBlank(*b1);
                        blank.m_size += b1->m_size;
                    }
                    
                    // space has been successfully reclaimed, now add the newly created blank
                    insertBlank(blank);
                    // remove the stripe if this alloc is full
                    if (alloc.isFull()) {
                        assert(alloc.hasStripe());
                        m_stripes.erase(it);
                        alloc.setHasStripe(false);
                    }
                    return true;
                }
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
            THROWF(db0::BadAddressException) << "Invalid address: " << address;            
        }
        assert(!alloc_window[1].isEnd());
        const auto alloc = *alloc_window[1].first;
        m_alloc_delta -= alloc.m_stride;
        // modify the central item (i.e. alloc_window[1])
        if (m_allocs.modify(alloc_window)->deallocUnit(address)) {
            if (alloc.stripeFlags()) {
                if (!alloc.hasStripe() && !redZone()) {
                    // no longer in the "red" zone, so we can safely register the stripe
                    // and reclaim the space
                    m_stripes.insert(alloc.toStripe());
                    m_allocs.modify(alloc_window)->setHasStripe(true);
                    m_alloc_delta -= alloc.capacity();
                    m_loss_delta -= alloc.capacity();
                }
            } else {
                // add stripe if it does not exist (and not lost)
                assert(!m_stripes.find_equal(alloc.toStripe()).first);
                // NOTE: we're unable to perform this operation when in the "red zone"
                // this will result in stripe-related address space irrecoverably lost
                if (redZone()) {
                    // need to mark the entire stripe's space as used (since it's unreachable to future allocs)                    
                    m_allocs.modify(alloc_window)->setLostStripe();
                    m_alloc_delta += alloc.capacity();
                    m_loss_delta += alloc.capacity();
                } else {
                    m_stripes.insert(alloc.toStripe());
                    m_allocs.modify(alloc_window)->setHasStripe(true);
                }
            }
            // just deallocated a single unit
            return;
        }
        
        // if the associated stripe exists then remove it
        if (criticalZone()) {
            // Do not perform any cleanups when in the critical zone
            // as it could result in exhausting the capacity available for metadata
            // This will result in increased fragmentation but address space is not lost
            return;
        }

        if (alloc.hasStripe()) {
            auto stripe = m_stripes.find_equal(alloc.toStripe());
            assert(stripe.first);            
            m_stripes.erase(stripe);            
            // NOTE: no need to remove the "has stripe" flag since alloc is to be removed
        }
        
        // we need to remove the alloc entry since it's empty
        std::optional<Blank> b0, b1;
        if (!alloc_window[0].isEnd()) {
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

        if (!alloc_window[2].isEnd()) {
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
            eraseBlank(*b0);
        }
        if (b1) {
            eraseBlank(*b1);
        }
        
        // L0 cache must be invalidated
        m_cache->clear();
        // remove the allocation
        m_allocs.erase(alloc_window[1]);
        if (m_allocs.empty()) {
            m_max_addr = 0;
        } else {
            m_max_addr = m_allocs.find_max().first->endAddr();
            assert(!m_bounds_fn || m_max_addr <= std::get<2>(m_bounds_fn()));
        }
        // re-insert the merged blank
        if (!b0) {
            b0 = Blank(alloc.size(), alloc.m_address);
        }
        if (!b1) {
            b1 = Blank(alloc.size(), alloc.m_address);
        }
        
        // the combined blanks size
        auto blank_size = b1->m_address + b1->m_size - b0->m_address;
        insertBlank({ blank_size, b0->m_address });
    }
    
    std::size_t CRDT_Allocator::getAllocSize(std::uint64_t address) const
    {
        auto alloc = m_allocs.lower_equal_bound(address);
        if (alloc.isEnd()) {
            THROWF(db0::BadAddressException) << "Invalid address: " << address;            
        }
        return alloc.first->getAllocSize(address);
    }
    
    bool CRDT_Allocator::isAllocated(std::uint64_t address, std::size_t *size_of_result) const
    {
        auto alloc = m_allocs.lower_equal_bound(address);
        if (alloc.isEnd()) {
            return false;
        }
        return alloc.first->isAllocated(address, size_of_result);
    }

    std::optional<std::uint32_t> CRDT_Allocator::tryAlignedAllocFromBlanks(std::uint32_t size)
    {
        // operation only allowed when in the "green zone"
        assert(greenZone());
        assert(size >= m_min_aligned_alloc_size);
        std::optional<Blank> blank;
        // for small allocations (1 < DP) try retrieving from the aligned blanks first
        if (size < m_page_size * ALIGNED_INDEX_THRESHOLD) {
            blank = tryPullBlank(m_aligned_blanks, size);
        }
        // if not present, then resort to regular blanks using adjusted blank size (to guarantee alignment)                
        if (!blank) {
            // blank size must be at least size + page size - 1
            blank = tryPullBlank(m_blanks, size + m_page_size - 1);
        }

        if (!blank) {
            return std::nullopt;
        }
        
        // L0 cache must be invalidated
        m_cache->clear();
        assert(blank->getAlignedSize(m_mask, m_page_size) >= size);
        auto addr = blank->getAlignedAddress(m_mask, m_page_size);
        
        // max_addr must be updated before any updates to allocator's metadata
        m_max_addr = std::max(m_max_addr, addr + size);
        
        assert(addr >= blank->m_address);
        assert(addr + size <= blank->m_address + blank->m_size);
        // NOTE: has_stripe flag is set here
        auto alloc = m_allocs.emplace(addr, size, 1u, true);
        auto result = alloc.first->allocUnit();
        assert(alloc.first->endAddr() <= m_max_addr);
        assert(!redZone());
        m_stripes.insert(alloc.first->toStripe());
        
        // give back the part of the blank before the allocated (aligned) address
        if (blank->m_address < addr) {
            insertBlank({ addr - blank->m_address, blank->m_address });
        }

        // give back the part of the blank after the allocated address
        assert(addr + size <= blank->m_address + blank->m_size);
        if (blank->m_address + blank->m_size > addr + size) {
            insertBlank({ blank->m_address + blank->m_size - addr - size, addr + size });
        }

        return result;
    }
    
    std::optional<std::uint32_t> CRDT_Allocator::tryAllocFromBlanks(std::uint32_t stride, std::uint32_t count)
    {
        // operation only allowed when in the "green zone"
        assert(greenZone());

        // Find the 1st blank of sufficient size (i.e. >= stride * count)
        auto min_size = stride * count;
        auto blank = tryPullBlank(m_blanks, min_size);
        if (!blank) {
            return std::nullopt;
        }
        
        // L0 cache must be invalidated
        m_cache->clear();

        // max_addr must be updated before any updates to allocator's metadata
        m_max_addr = std::max(m_max_addr, blank->m_address + min_size);
        // NOTE: has_stripe flag is set here
        auto alloc = m_allocs.emplace((std::uint32_t)blank->m_address, stride, count, true);
        auto result = alloc.first->allocUnit();
        assert(alloc.first->endAddr() <= m_max_addr);
        assert(!redZone());
        if (count > 1) {
            // register with L0 cache
            m_cache->addMutable(alloc, alloc.first);
        }
        // register the new alloc with stripes (even if count == 1)        
        m_stripes.insert(alloc.first->toStripe());

        if (blank->m_size > min_size) {
            // register remaining part of the blank
            // note that the remaining part is registered even if it falls outside of the dynamic bounds
            // this is by design since the dynamic bounds may change in the future            
            insertBlank({ blank->m_size - stride * count, blank->m_address + stride * count });
        }
        return result;
    }
    
    std::optional<std::uint32_t> CRDT_Allocator::tryAllocFromStripe(typename StripeSetT::ConstItemIterator &stripe,
        std::uint32_t &last_stripe_units, std::optional<std::uint32_t> &addr_bound)
    {
        // find the corresponding alloc next
        auto alloc = m_allocs.find_equal(stripe.first->m_address);
        if (!alloc.first) {
            assert(false);
            THROWF(db0::InternalException) << "CRDT_Allocator internal error: alloc not found";
        }
        
        if (alloc.first->isFull()) {
            last_stripe_units = alloc.first->getUnitCount();
            assert(alloc.first->hasStripe());
            // remove from stripes
            assert(stripe.validate());
            m_stripes.erase(stripe);
            // clear the has_stripe flag
            auto alloc_ptr = m_allocs.modify(alloc);
            alloc_ptr->setHasStripe(false);
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
            addr_bound = std::get<0>(m_bounds_fn());
        }
        
        auto result = m_cache->tryAlloc(size, addr_bound);
        if (result) {
            return result;
        }
        
        // Find stripe of exactly matching size
        last_stripe_units = 0;
        auto stripe_ptr = m_stripes.lower_equal_bound(size);
        assert(stripe_ptr.validate());
        if (stripe_ptr.isEnd() || stripe_ptr.first->m_stride != size) {
            return std::nullopt;
        }
        
        result = tryAllocFromStripe(stripe_ptr, last_stripe_units, addr_bound);
        if (last_stripe_units > 0 || result) {
            return result;
        }
        
        // try with other registered stripes (which might be within the dynamic bounds)
        auto it = m_stripes.upper_slice(stripe_ptr);
        auto node = it.get().second;
        assert(!it.is_end());
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
    
    std::uint64_t CRDT_Allocator::alloc(std::size_t size, bool align)
    {
        auto result = tryAlloc(size, align);
        if (!result) {
            THROWF(InternalException) << "CRDT_Allocator: out of memory" << THROWF_END;
        }
        return *result;
    }
    
    void CRDT_Allocator::setDynamicBound(BoundsFunctionT bounds_fn) {
        this->m_bounds_fn = bounds_fn;
    }

    std::uint64_t CRDT_Allocator::getFirstAddress() {
        return 0;
    }
    
    void CRDT_Allocator::commit() const {
        m_cache->clear();
    }
    
    void CRDT_Allocator::detach() const {
        m_cache->clear();
    }
    
    std::uint32_t CRDT_Allocator::Blank::getAlignedAddress(std::uint32_t mask, std::uint32_t page_size) const
    {
        if (m_address & mask) {
            auto result = (m_address & ~mask) + page_size;
            assert(result < m_address + m_size);
            return result;
        } else {
            // already aligned
            return m_address;
        }
    }

    std::uint32_t CRDT_Allocator::Blank::getAlignedSize(std::uint32_t mask, std::uint32_t page_size) const
    {
        if (m_address & mask) {
            auto aligned_addr = (m_address & ~mask) + page_size;
            if (aligned_addr < m_address + m_size) {
                return m_size - (aligned_addr - m_address);
            } else {
                // non-aligned blank
                return 0;
            }
        } 
        // already aligned
        return m_size;
    }
    
    bool CRDT_Allocator::isAligned(const Blank &blank) const
    {
        auto aligned_size = blank.getAlignedSize(m_mask, m_page_size);
        // the upper boundary is arbitrary
        return aligned_size >= m_min_aligned_alloc_size && aligned_size < m_page_size * ALIGNED_INDEX_THRESHOLD;
    }

    bool CRDT_Allocator::isAligned(const Blank &blank, std::uint32_t page_size, std::optional<std::uint32_t> min_aligned_alloc_size)
    {
        auto aligned_size = blank.getAlignedSize(getPageMask(page_size), page_size);
        return aligned_size > getMinAlignedAllocSize(min_aligned_alloc_size, page_size) && aligned_size < page_size * ALIGNED_INDEX_THRESHOLD;
    }
    
    bool CRDT_Allocator::redZone() const
    {
        assert(!m_bounds_fn || m_max_addr <= std::get<2>(m_bounds_fn()));
        return m_bounds_fn && m_max_addr >= std::get<1>(m_bounds_fn());
    }
    
    bool CRDT_Allocator::greenZone() const {
        return !m_bounds_fn || m_max_addr < std::get<0>(m_bounds_fn());
    }
    
    bool CRDT_Allocator::criticalZone() const {
        return m_bounds_fn && m_max_addr > (std::get<2>(m_bounds_fn()) - m_critical_margin);
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
