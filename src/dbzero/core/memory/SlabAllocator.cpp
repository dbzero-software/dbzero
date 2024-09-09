#include "SlabAllocator.hpp"
#include "OneShotAllocator.hpp"
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{

    SlabAllocator::SlabAllocator(std::shared_ptr<Prefix> prefix, std::uint64_t begin_addr, std::uint32_t size,
        std::size_t page_size, std::optional<std::size_t> remaining_capacity)
        : m_prefix(prefix)
        , m_begin_addr(begin_addr)
        , m_page_size(page_size)
        , m_slab_size(size)
        , m_internal_memspace(prefix, nullptr)
        // header starts at the beginning of the slab
        , m_header(m_internal_memspace.myPtr(headerAddr(begin_addr, size)))
        // bitspace starts after the header
        , m_bitspace(prefix, headerAddr(begin_addr, size), page_size, -1)
        // calculate relative pointers to CRDT Allocator data structures
        , m_allocs(m_bitspace.myPtr(begin_addr + m_header->m_alloc_set_ptr), page_size)
        , m_blanks(m_bitspace.myPtr(begin_addr + m_header->m_blank_set_ptr), page_size)
        , m_aligned_blanks(m_bitspace.myPtr(begin_addr + m_header->m_aligned_blank_set_ptr), page_size, CompT(page_size), page_size)
        , m_stripes(m_bitspace.myPtr(begin_addr + m_header->m_stripe_set_ptr), page_size)
        , m_instance_ids(m_bitspace.myPtr(begin_addr + m_header->m_instance_ids_ptr), page_size)
        , m_allocator(m_allocs, m_blanks, m_aligned_blanks, m_stripes, m_header->m_size, page_size)
        , m_initial_remaining_capacity(remaining_capacity)
        , m_initial_admin_size(getAdminSpaceSize(true))
    {
        // For aligned allocations the begin address must be also aligned
        assert(m_begin_addr % m_page_size == 0);
        // apply dynamic bound on the CRDT allocator to not assign addresses overlapping with the admin space
        // include ADMIN_SPAN bitspace allocations to allow margin for the admin space to grow
        std::uint64_t bounds_base = m_slab_size - (m_begin_addr + m_slab_size - m_bitspace.getBaseAddress() + ADMIN_SPAN * m_page_size);
        m_allocator.setDynamicBound([this, bounds_base]() {
            return bounds_base - m_bitspace.span() * m_page_size;
        });

        // provide CRDT allocator's dynamic bound to the bitspace (this is for address validation/ collision prevention purposes)
        m_bitspace.setDynamicBounds([this]() {
            return m_begin_addr + m_allocator.getMaxAddr();
        });
    }
    
    SlabAllocator::~SlabAllocator()
    {
        if (m_on_close_handler) {
            m_on_close_handler(*this);
        }
    }

    std::optional<std::uint64_t> SlabAllocator::tryAlloc(std::size_t size, std::uint32_t slot_num, bool aligned)
    {
        assert(slot_num == 0);
        assert(size > 0);        
        // obtain relative address from the underlying CRDT allocator
        // auto-align when requested size > page_size
        auto relative = m_allocator.tryAlloc(size, (size > m_page_size) || aligned);
        if (relative) {
            m_alloc_delta += size;
            return makeAbsolute(*relative);
        }
        return std::nullopt;
    }
    
    void SlabAllocator::free(std::uint64_t address) {
        m_allocator.free(makeRelative(address));
    }
    
    std::size_t SlabAllocator::getAllocSize(std::uint64_t address) const {
        return m_allocator.getAllocSize(makeRelative(address));
    }

    std::uint64_t SlabAllocator::headerAddr(std::uint64_t begin_addr, std::uint32_t size) {
        return begin_addr + size - o_slab_header::sizeOf();
    }
    
    std::size_t SlabAllocator::formatSlab(std::shared_ptr<Prefix> prefix, std::uint64_t begin_addr, std::uint32_t size, std::size_t page_size)
    {   
        auto admin_size = calculateAdminSpaceSize(page_size);
        if (admin_size + ADMIN_SPAN * page_size >= size) {
            THROWF(db0::InternalException) << "Slab size too small: " << size;
        }

        if (size % page_size != 0) {
            THROWF(db0::InternalException) << "Slab size not multiple of page size: " << size << " % " << page_size;
        }

        // put bitspace right before the header (at the end of the slab )
        BitSpace<SLAB_BITSPACE_SIZE()>::create(prefix, headerAddr(begin_addr, size), page_size, -1);
        // open newly created bitspace
        // use offset = begin_addr (to allow storing internal addresses as 32bit)
        BitSpace<SLAB_BITSPACE_SIZE()> bitspace(prefix, headerAddr(begin_addr, size), page_size, -1);
        
        // create the CRDT allocator data structures on top of the bitspace
        AllocSetT allocs(bitspace, page_size);
        BlankSetT blanks(bitspace, page_size);
        AlignedBlankSetT aligned_blanks(bitspace, page_size, CompT(page_size), page_size);
        StripeSetT stripes(bitspace, page_size);
        LimitedVector<std::uint16_t> instance_ids(bitspace, page_size);
        // calculate size initially available to CRTD allocator
        std::uint32_t crdt_size = static_cast<std::uint32_t>(size - admin_size - ADMIN_SPAN * page_size);
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
            instance_ids.getAddress() - begin_addr
            );
        return crdt_size;
    }
    
    const std::size_t SlabAllocator::getSlabSize() const {
        return m_slab_size;
    }

    const std::size_t SlabAllocator::getAdminSpaceSize(bool include_margin) const 
    {
        // add +ADMIN_SPAN bitspace allocations to allow growth of the CRDT collections
        auto result = m_begin_addr + m_slab_size - m_bitspace.getBaseAddress() + m_bitspace.span() * m_page_size;
        if (include_margin) {
            result += ADMIN_SPAN * m_page_size;
        }
        return result;
    }

    std::size_t SlabAllocator::calculateAdminSpaceSize(std::size_t page_size)
    {
        auto result = BitSpace<SLAB_BITSPACE_SIZE()>::sizeOf() + o_slab_header::sizeOf();
        // round to full page size
        result = (result + page_size - 1) / page_size * page_size;
        // add ADMIN_SPAN pages for CRDT types
        result += page_size * ADMIN_SPAN;
        return result;
    }

    std::size_t SlabAllocator::getMaxAllocSize() const {
        return m_slab_size - calculateAdminSpaceSize(m_page_size) - ADMIN_SPAN * m_page_size;
    }
    
    std::size_t SlabAllocator::getRemainingCapacity() const 
    {
        if (!m_initial_remaining_capacity) {
            THROWF(db0::InternalException) << "SlabAllocator::getRemainingCapacity() called on a slab without initial capacity";
        } 
        std::int64_t result = *m_initial_remaining_capacity - m_allocator.getAllocDelta() - (getAdminSpaceSize(true) - m_initial_admin_size);
        return result > 0 ? result : 0;
    }
    
    const Prefix &SlabAllocator::getPrefix() const {
        return *m_prefix;
    }
    
    void SlabAllocator::setOnCloseHandler(std::function<void(const SlabAllocator &)> handler) {
        m_on_close_handler = handler;
    }
    
    bool SlabAllocator::empty() const {
        return m_allocs.empty();
    }

    std::uint64_t SlabAllocator::getAddress() const {
        return m_begin_addr;
    }

    std::uint32_t SlabAllocator::size() const {
        return m_slab_size;
    }

    std::uint64_t SlabAllocator::getFirstAddress() {
        return CRDT_Allocator::getFirstAddress();
    }
    
    void SlabAllocator::commit()
    {
        m_header.commit();
        m_allocs.commit();
        m_blanks.commit();
        m_aligned_blanks.commit();
        m_stripes.commit();
        m_allocator.commit();
    }
    
    void SlabAllocator::detach() const
    {
        m_header.detach();
        m_allocs.detach();
        m_blanks.detach();
        m_aligned_blanks.detach();
        m_stripes.detach();
        m_allocator.detach();
    }

}