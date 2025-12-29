// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "SlabAllocator.hpp"
#include "OneShotAllocator.hpp"
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{

    using offset_t = typename Address::offset_t;

    SlabAllocator::SlabAllocator(std::shared_ptr<Prefix> prefix, Address begin_addr, std::uint32_t size,
        std::size_t page_size, std::optional<std::size_t> remaining_capacity, std::optional<std::size_t> lost_capacity)
        : m_prefix(prefix)
        , m_begin_addr(begin_addr)
        , m_page_size(page_size)
        , m_page_shift(getPageShift(page_size))
        , m_slab_size(size)
        , m_internal_memspace(prefix, nullptr)
        // header starts at the beginning of the slab
        , m_header(m_internal_memspace.myPtr(headerAddr(begin_addr, size)))
        // bitspace starts after the header
        , m_bitspace(prefix, headerAddr(begin_addr, size), page_size, -1)
        // calculate relative pointers to CRDT Allocator data structures
        , m_allocs(m_bitspace.myPtr(begin_addr + static_cast<offset_t>(m_header->m_alloc_set_ptr)), page_size)
        , m_blanks(m_bitspace.myPtr(begin_addr + static_cast<offset_t>(m_header->m_blank_set_ptr)), page_size)
        , m_aligned_blanks(m_bitspace.myPtr(begin_addr + static_cast<offset_t>(m_header->m_aligned_blank_set_ptr)),
            page_size, CompT(page_size), page_size)
        , m_stripes(m_bitspace.myPtr(begin_addr + static_cast<offset_t>(m_header->m_stripe_set_ptr)), page_size)
        // use 14 bit page level allocation counters
        , m_alloc_counter(m_bitspace.myPtr(begin_addr + static_cast<offset_t>(m_header->m_alloc_counter_ptr)), page_size, (1u << 14) - 1)
        , m_allocator(m_allocs, m_blanks, m_aligned_blanks, m_stripes, m_header->m_size, page_size)
        , m_initial_remaining_capacity(remaining_capacity)
        , m_initial_lost_capacity(lost_capacity)
        , m_initial_admin_size(getAdminSpaceSize(true))
    {
        // For aligned allocations the begin address must be also aligned
        assert(m_begin_addr % m_page_size == 0);
        
        // apply dynamic bound on the CRDT allocator to prevent allocating addresses overlapping with the admin space
        // include ADMIN_MARGIN bitspace allocations to allow margin for the admin space to grow
        // NOTE: CRDT allocator's dynamic bounds are specified as relative to the allocator's base address
        std::uint64_t bounds_base = makeRelative(m_bitspace.getBaseAddress());
        auto admin_margin_bytes = ADMIN_MARGIN() << m_page_shift;
        // returns recommended & hard bound
        m_allocator.setDynamicBound([this, bounds_base, admin_margin_bytes]() {
            std::uint32_t b2 = bounds_base - (m_bitspace.span() << m_page_shift);
            std::uint32_t b1 = (b2 >= admin_margin_bytes) ? b2 - admin_margin_bytes : 0;
            std::uint32_t b0 = (b1 >= admin_margin_bytes) ? b1 - admin_margin_bytes : 0;
            return std::make_tuple(b0, b1, b2);
        });
        
        // provide CRDT allocator's dynamic bound to the bitspace (this is for address validation/ collision prevention purposes)
        // NOTE: the bitspace bounds use the absolute address
        m_bitspace.setDynamicBounds([this]() {
            return m_begin_addr + static_cast<offset_t>(m_allocator.getMaxAddr());
        });
    }
    
    SlabAllocator::~SlabAllocator()
    {
    }
    
    std::optional<Address> SlabAllocator::tryAlloc(std::size_t size, std::uint32_t slot_num,
        bool aligned, unsigned char, unsigned char)
    {
        assert(slot_num == 0);
        assert(size > 0);
        // obtain relative address from the underlying CRDT allocator
        // auto-align when requested size > page_size
        auto relative = m_allocator.tryAlloc(size, (size > m_page_size) || aligned);
        if (relative) {
            return makeAbsolute(*relative);
        }
        return std::nullopt;
    }
    
    void SlabAllocator::free(Address address) {
        m_allocator.free(makeRelative(address));
    }
    
    std::size_t SlabAllocator::getAllocSize(Address address) const {
        return m_allocator.getAllocSize(makeRelative(address));
    }

    std::size_t SlabAllocator::getAllocSize(Address address, unsigned char) const {
        return m_allocator.getAllocSize(makeRelative(address));
    }

    bool SlabAllocator::isAllocated(Address address, std::size_t *size_of_result) const {
        return m_allocator.isAllocated(makeRelative(address), size_of_result);
    }
    
    bool SlabAllocator::isAllocated(Address address, unsigned char, std::size_t *size_of_result) const {
        return m_allocator.isAllocated(makeRelative(address), size_of_result);
    }
    
    Address SlabAllocator::headerAddr(Address begin_addr, std::uint32_t size) {
        return begin_addr + static_cast<offset_t>(size) - static_cast<offset_t>(o_slab_header::sizeOf());
    }
    
    std::size_t SlabAllocator::formatSlab(std::shared_ptr<Prefix> prefix, Address begin_addr, 
        std::uint32_t size, std::size_t page_size)
    {
        auto admin_size = calculateAdminSpaceSize(page_size);
        auto admin_margin_bytes = 2 * ADMIN_MARGIN() * page_size;
        if (size <= admin_size + admin_margin_bytes) {
            THROWF(db0::InternalException) << "Slab size too small: " << size;
        }
        
        if (size % page_size != 0) {
            THROWF(db0::InternalException) << "Slab size not multiple of page size: " << size << " % " << page_size;
        }
        
        // put bitspace right before the header (at the end of the slab)
        BitSpace<SlabAllocatorConfig::SLAB_BITSPACE_SIZE()>::create(
            prefix, headerAddr(begin_addr, size), page_size, -1
        );
        // open newly created bitspace
        // use offset = begin_addr (to allow storing internal addresses as 32bit)
        BitSpace<SlabAllocatorConfig::SLAB_BITSPACE_SIZE()> bitspace(
            prefix, headerAddr(begin_addr, size), page_size, -1
        );
        
        // Create the CRDT allocator data structures on top of the bitspace
        AllocSetT allocs(bitspace, page_size);
        BlankSetT blanks(bitspace, page_size);
        AlignedBlankSetT aligned_blanks(bitspace, page_size, CompT(page_size), page_size);
        StripeSetT stripes(bitspace, page_size);
        LimitedVector<std::uint16_t> alloc_counter(bitspace, page_size);        
        alloc_counter.reserve(SlabAllocatorConfig::SLAB_BITSPACE_SIZE());
        // calculate size initially available to CRTD allocator
        std::uint32_t crdt_size = static_cast<std::uint32_t>(size - admin_size - admin_margin_bytes);
        assert(crdt_size > 0);
        
        // register the initial blank - associated with the relative address = 0
        CRDT_Allocator::insertBlank(blanks, aligned_blanks, { crdt_size, 0 }, page_size);
        
        // create a temporary memspace only to allocate the header under a known address
        OneShotAllocator osa(headerAddr(begin_addr, size), o_slab_header::sizeOf());
        Memspace memspace(Memspace::tag_from_reference(), prefix, osa);
        // finally create the Slab header
        v_object<o_slab_header> header(
            memspace,
            crdt_size,
            // assign addresses relative to the slab beginning
            allocs.getAddress() - begin_addr,
            blanks.getAddress() - begin_addr,
            aligned_blanks.getAddress() - begin_addr,
            stripes.getAddress() - begin_addr,
            alloc_counter.getAddress() - begin_addr
            );
        return crdt_size;
    }
    
    const std::size_t SlabAllocator::getSlabSize() const {
        return m_slab_size;
    }
    
    const std::size_t SlabAllocator::getAdminSpaceSize(bool include_margin) const
    {
        auto result = m_begin_addr.getOffset() + m_slab_size - m_bitspace.getBaseAddress() + m_bitspace.span() * m_page_size;
        // add +ADMIN_MARGIN bitspace allocations to allow growth of the CRDT collections
        if (include_margin) {
            result += 2 * ADMIN_MARGIN() * m_page_size;
        }
        return result;
    }
    
    std::size_t SlabAllocator::calculateAdminSpaceSize(std::size_t page_size)
    {
        auto result = BitSpace<SlabAllocatorConfig::SLAB_BITSPACE_SIZE()>::sizeOf() + o_slab_header::sizeOf();
        // round to full page size
        result = (result + page_size - 1) / page_size * page_size;
        // add ADMIN_SPAN pages for CRDT types (actual space initially occupied)
        result += page_size * ADMIN_SPAN();
        // include limited vector's reserved capacity
        result += LimitedVectorT::DP_REQ(SlabAllocatorConfig::SLAB_BITSPACE_SIZE(), page_size) * page_size;
        return result;
    }
    
    std::size_t SlabAllocator::getMaxAllocSize() const {
        return m_slab_size - calculateAdminSpaceSize(m_page_size) - ADMIN_MARGIN() * m_page_size;
    }
    
    std::size_t SlabAllocator::getLostCapacity() const
    {
        if (!m_initial_lost_capacity) {
            THROWF(db0::InternalException) << "SlabAllocator::getLostCapacity() called on a slab without initial lost capacity";
        }
        std::int64_t result = (std::int64_t)*m_initial_lost_capacity + m_allocator.getLossDelta();
        assert(result >= 0);
        return static_cast<std::size_t>(result);
    }
    
    std::size_t SlabAllocator::getRemainingCapacity() const
    {
        if (!m_initial_remaining_capacity) {
            THROWF(db0::InternalException) << "SlabAllocator::getRemainingCapacity() called on a slab without initial capacity";
        }
        std::int64_t result = (std::int64_t)*m_initial_remaining_capacity - m_allocator.getAllocDelta() - (getAdminSpaceSize(true) - m_initial_admin_size);
        return result > 0 ? result : 0;
    }
    
    const Prefix &SlabAllocator::getPrefix() const {
        return *m_prefix;
    }
        
    bool SlabAllocator::empty() const {
        return m_allocs.empty();
    }

    Address SlabAllocator::getAddress() const {
        return m_begin_addr;
    }

    std::uint32_t SlabAllocator::size() const {
        return m_slab_size;
    }

    Address SlabAllocator::getFirstAddress() {
        return Address::fromOffset(CRDT_Allocator::getFirstAddress());
    }
    
    void SlabAllocator::commit() const
    {
        m_header.commit();
        m_bitspace.commit();
        m_allocs.commit();
        m_blanks.commit();
        m_aligned_blanks.commit();
        m_stripes.commit();
        m_alloc_counter.commit();
        m_allocator.commit();        
    }
    
    void SlabAllocator::detach() const
    {
        m_header.detach();
        m_bitspace.detach();
        m_allocs.detach();
        m_blanks.detach();
        m_aligned_blanks.detach();
        m_stripes.detach();
        m_alloc_counter.detach();
        m_allocator.detach();
    }
    
    bool SlabAllocator::tryMakeAddressUnique(Address address, std::uint16_t &instance_id)
    {
        // make sure high 14 bits are 0
        assert((address.getOffset() >> 50) == 0 && "SlabAllocator: address space exhausted");
        auto page_id = makeRelative(address) >> m_page_shift;
        if (!m_alloc_counter.atomicInc(page_id, instance_id)) {
            // makeAddressUnique failed due to counter overflow
            return false;
        }
        assert(instance_id > 0);
        assert(instance_id <= UniqueAddress::INSTANCE_ID_MAX);
        
        return true;
    }
    
    UniqueAddress SlabAllocator::tryMakeAddressUnique(Address address)
    {
        std::uint16_t instance_id;
        if (tryMakeAddressUnique(address, instance_id)) {
            return { address, instance_id };
        }
        // unable to make the address unique
        return {};
    }
    
    bool SlabAllocator::inRange(Address address) const 
    {
        return (address.getOffset() >= m_begin_addr.getOffset()) && 
            (address.getOffset() < m_begin_addr.getOffset() + m_slab_size);
    }
    
    std::pair<Address, std::optional<Address> > SlabAllocator::getRange(std::uint32_t slot_num) const
    {
        assert(!slot_num && "SlabAllocator does not support slots");
        return { m_begin_addr, m_begin_addr + static_cast<offset_t>(m_slab_size) };
    }
    
}