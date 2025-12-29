// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "MetaAllocator.hpp"
#include "OneShotAllocator.hpp"
#include "Memspace.hpp"
#include "SlabManager.hpp"
#include <unordered_map>
#include <dbzero/core/vspace/v_object.hpp>

namespace db0

{

    static constexpr double MIN_FILL_RATE = 0.25;
    
    inline unsigned char getRealmID(std::uint32_t slab_id) {
        return slab_id & MetaAllocator::REALM_MASK;
    }

    std::size_t MetaAllocator::getSlabCount(std::size_t page_size, std::size_t slab_size)
    {
        auto MP = 2 * NUM_REALMS; // number of meta pages
        std::size_t max_slab_count = (std::numeric_limits<std::uint32_t>::max() - MP * page_size) / slab_size - 1;
        // estimate the number of slabs for which the definitions can be stored on a single page
        // this is a very conservative estimate
        std::size_t slab_count_1 = (std::size_t)(MIN_FILL_RATE * (double)page_size / (double)sizeof(SlabDef));
        std::size_t slab_count_2 = (std::size_t)(MIN_FILL_RATE * (double)page_size / (double)sizeof(CapacityItem)) - (2 * MP);
        return std::min(max_slab_count, std::min(slab_count_1, slab_count_2));
    }

    std::size_t align(std::size_t address, std::size_t page_size) {
        return ((address + page_size - 1) / page_size) * page_size;
    }
    
    std::function<Address(unsigned int)> MetaAllocator::getAddressPool(std::size_t offset, std::size_t page_size, 
        std::size_t slab_size) 
    {
        auto slab_count = getSlabCount(page_size, slab_size);
        // make offset page-aligned
        offset = align(offset, page_size);
        // take the first 2 pages (* NUM_REALMS) before a sequence of slabs for metadata
        // MP = number of meta pages
        auto MP = 2 * NUM_REALMS;
        return [offset, slab_count, page_size, slab_size, MP](unsigned int i) -> Address {
            assert(MP * page_size + slab_size * slab_count < std::numeric_limits<std::uint32_t>::max());
            return Address::fromOffset(
                (std::uint64_t)(i / MP) * (MP * page_size + slab_size * slab_count) + offset + (std::uint64_t)(i % MP) * page_size
            );
        };
    }

    // Construct the reverse address pool function
    std::function<unsigned int(Address)> MetaAllocator::getReverseAddressPool(std::size_t offset, 
        std::size_t page_size, std::size_t slab_size)
    {
        auto slab_count = getSlabCount(page_size, slab_size);
        // make offset page-aligned
        offset = align(offset, page_size);
        // MP = number of meta pages
        auto MP = 2 * NUM_REALMS;
        return [offset, slab_count, page_size, slab_size, MP](Address addr) -> unsigned int {
            auto x = MP * ((addr - offset) / (MP * page_size + slab_size * slab_count));
            auto d = (addr - offset) % (MP * page_size + slab_size * slab_count);
            if (d % page_size != 0) {
                THROWF(db0::InternalException) << "Invalid meta-address pool address: " << addr;
            }
            assert (d < MP * page_size);
            return x + (d / page_size);
        };
    }
    
    std::function<std::uint32_t(Address)> MetaAllocator::getSlabIdFunction(std::size_t offset, std::size_t page_size,
        std::size_t slab_size)
    {
        auto slab_count = getSlabCount(page_size, slab_size);
        offset = align(offset, page_size);
        auto MP = 2 * NUM_REALMS;
        auto block_size = MP * page_size + slab_size * slab_count;
        return [offset, page_size, slab_size, slab_count, block_size, MP](Address address) -> std::uint32_t {
            auto block_id = (address - offset) / block_size;
            auto slab_num = (address - offset - block_id * block_size - MP * page_size) / slab_size;
            return block_id * slab_count + slab_num;
        }; 
    }
    
    // Get function to translate slab id to slab address
    std::function<Address(unsigned int)> getSlabAddressFunction(std::size_t offset, std::size_t page_size, std::size_t slab_size)
    {
        auto slab_count = MetaAllocator::getSlabCount(page_size, slab_size);
        // make offset page-aligned
        offset = align(offset, page_size);
        auto MP = 2 * MetaAllocator::NUM_REALMS;
        auto block_size = MP * page_size + slab_size * slab_count;
        return [offset, slab_count, page_size, slab_size, block_size, MP](unsigned int i) -> Address {
            auto block_id = i / slab_count;
            auto slab_num = i % slab_count;
            return Address::fromOffset(offset + block_id * block_size + MP * page_size + slab_num * slab_size);
        };
    }
    
    o_meta_header::o_meta_header(std::uint32_t page_size, std::uint32_t slab_size)
        : m_page_size(page_size)
        , m_slab_size(slab_size)
    {
    }
    
    std::uint64_t MetaAllocator::Realm::getSlabMaxAddress() const
    {
        // take max of the 2 collections
        std::uint64_t max_addr = std::max(
            m_slab_defs.getAddress(), m_capacity_items.getAddress()
        );
        // and their items ...
        for (auto it = m_slab_defs.cbegin_nodes(), end = m_slab_defs.cend_nodes(); it != end; ++it) {
            max_addr = std::max(max_addr, it.getAddress().getOffset());
        }
        for (auto it = m_capacity_items.cbegin_nodes(), end = m_capacity_items.cend_nodes(); it != end; ++it) {
            max_addr = std::max(max_addr, it.getAddress().getOffset());
        }
        return max_addr;
    }
    
    MetaAllocator::MetaAllocator(std::shared_ptr<Prefix> prefix, SlabRecycler *recycler, bool deferred_free)
        : m_prefix(prefix)
        , m_header(getMetaHeader(prefix))
        , m_algo_allocator(
            getAddressPool(o_meta_header::sizeOf(), m_header.m_page_size, m_header.m_slab_size),
            getReverseAddressPool(o_meta_header::sizeOf(), m_header.m_page_size, m_header.m_slab_size),
            m_header.m_page_size
        )
        , m_metaspace(createMetaspace())
        , m_realms(m_metaspace, m_prefix, recycler, m_header, NUM_REALMS, deferred_free)
        , m_recycler_ptr(recycler)        
        , m_slab_id_function(getSlabIdFunction(o_meta_header::sizeOf(), m_header.m_page_size, m_header.m_slab_size))
    {
        auto max_addr = m_realms.getSlabMaxAddress();
        // forward this address to the meta-space
        if (max_addr > 0) {
            m_algo_allocator.setMaxAddress(Address::fromOffset(max_addr));
        }
    }
    
    MetaAllocator::~MetaAllocator()
    {
    }
    
    MetaAllocator::Realm::Realm(Memspace &metaspace, std::shared_ptr<Prefix> prefix, SlabRecycler *slab_recycler,
        o_realm realm, std::uint32_t slab_size, std::uint32_t page_size, unsigned char realm_id, bool deferred_free)
        : m_slab_defs(metaspace.myPtr(realm.m_slab_defs_ptr), page_size)
        , m_capacity_items(metaspace.myPtr(realm.m_capacity_items_ptr), page_size)
        , m_slab_manager(std::make_unique<SlabManager>(prefix, m_slab_defs, m_capacity_items, slab_recycler,
            slab_size,
            page_size,
            getSlabAddressFunction(o_meta_header::sizeOf(), page_size, slab_size),
            getSlabIdFunction(o_meta_header::sizeOf(), page_size, slab_size),
            realm_id,
            deferred_free
        ))
    {
    }
    
    Memspace MetaAllocator::createMetaspace() const
    {
        // this is to temporarily initialize for unlimited reading
        auto get_address = getAddressPool(o_meta_header::sizeOf(), m_header.m_page_size, m_header.m_slab_size);
        m_algo_allocator.setMaxAddress(get_address(std::numeric_limits<unsigned int>::max() - 1));
        return { Memspace::tag_from_reference(), m_prefix, m_algo_allocator };
    }
    
    void MetaAllocator::formatPrefix(std::shared_ptr<Prefix> prefix, std::size_t page_size, std::size_t slab_size)
    {
        // create the meta-header at the address 0x0
        OneShotAllocator one_shot(Address::fromOffset(0), o_meta_header::sizeOf());
        Memspace memspace(Memspace::tag_from_reference(), prefix, one_shot);
        v_object<o_meta_header> meta_header(memspace, page_size, slab_size);
        auto offset = o_meta_header::sizeOf();
        // Construct the meta-space for the slab tree
        AlgoAllocator algo_allocator(
                getAddressPool(offset, page_size, slab_size), 
                getReverseAddressPool(offset, page_size, slab_size), 
                page_size);
        Memspace meta_space(Memspace::tag_from_reference(), prefix, algo_allocator);
        
        // initialize realms in the meta-header
        for (unsigned int i = 0; i < NUM_REALMS; ++i) {
            // Create the empty slab-defs and capacity items trees on the meta-space
            SlabTreeT slab_defs(meta_space, page_size);
            CapacityTreeT capacity_items(meta_space, page_size);

            // and put their addresses in the header...
            meta_header.modify().m_realms[i].m_slab_defs_ptr = Address::fromOffset(slab_defs.getAddress());
            meta_header.modify().m_realms[i].m_capacity_items_ptr = Address::fromOffset(capacity_items.getAddress());
        }
    }
    
    o_meta_header MetaAllocator::getMetaHeader(std::shared_ptr<Prefix> prefix)
    {
        // meta-header is located at the fixed address 0x0
        auto header_addr = Address::fromOffset(0);
        OneShotAllocator one_shot(header_addr, o_meta_header::sizeOf());
        Memspace memspace(Memspace::tag_from_reference(), prefix, one_shot);
        v_object<o_meta_header> meta_header(memspace.myPtr(header_addr));
        return meta_header.const_ref();
    }
    
    std::optional<Address> MetaAllocator::tryAlloc(std::size_t size, std::uint32_t slot_num, 
        bool aligned, unsigned char realm_id, unsigned char locality)
    {
        std::uint16_t instance_id;
        return tryAllocImpl(size, slot_num, aligned, false, instance_id, realm_id, locality);
    }
    
    std::optional<UniqueAddress> MetaAllocator::tryAllocUnique(std::size_t size, std::uint32_t slot_num,
        bool aligned, unsigned char realm_id, unsigned char locality)
    {
        std::uint16_t instance_id;
        auto addr = tryAllocImpl(size, slot_num, aligned, true, instance_id, realm_id, locality);
        if (addr) {
            return UniqueAddress(*addr, instance_id);
        }
        return {};
    }
    
    std::optional<Address> MetaAllocator::tryAllocImpl(std::size_t size, std::uint32_t slot_num, bool aligned, bool unique,
        std::uint16_t &instance_id, unsigned char realm_id, unsigned char locality)
    {
        assert(slot_num == 0);
        assert(size > 0);        
        return m_realms[realm_id].tryAlloc(size, slot_num, aligned, unique, instance_id, locality);
    }
    
    void MetaAllocator::free(Address address)
    {        
        auto slab_id = m_slab_id_function(address);
        auto realm_id = getRealmID(slab_id);
        m_realms[realm_id].free(address, slab_id);
    }

    std::size_t MetaAllocator::getAllocSize(Address address) const
    {        
        auto slab_id = m_slab_id_function(address);
        auto realm_id = getRealmID(slab_id);
        return m_realms[realm_id].getAllocSize(address, slab_id);
    }
    
    std::size_t MetaAllocator::getAllocSize(Address address, unsigned char realm_id) const
    {
        auto slab_id = m_slab_id_function(address);
        if (realm_id != getRealmID(slab_id)) {
            THROWF(db0::BadAddressException) << "Invalid address accessed";
        }
        return m_realms[realm_id].getAllocSize(address, slab_id);
    }
    
    bool MetaAllocator::isAllocated(Address address, std::size_t *size_of_result) const
    {                
        auto slab_id = m_slab_id_function(address);
        auto realm_id = getRealmID(slab_id);
        return m_realms[realm_id].isAllocated(address, slab_id, size_of_result);
    }

    bool MetaAllocator::isAllocated(Address address, unsigned char realm_id, std::size_t *size_of_result) const
    {
        auto slab_id = m_slab_id_function(address);
        if (realm_id != getRealmID(slab_id)) {
            THROWF(db0::BadAddressException) << "Invalid address accessed";
        }
        return m_realms[realm_id].isAllocated(address, slab_id, size_of_result);
    }
    
    unsigned int MetaAllocator::getSlabCount() const
    {
        unsigned int total_slab_count = 0;
        for (unsigned int i = 0; i < MetaAllocator::NUM_REALMS; ++i) {
            total_slab_count += m_realms[i].getSlabCount();
        }
        return total_slab_count;
    }
    
    std::uint32_t MetaAllocator::getRemainingCapacity(std::uint32_t slab_id) const
    {
        auto realm_id = slab_id & MetaAllocator::REALM_MASK;
        return m_realms[realm_id].getRemainingCapacity(slab_id);
    }
    
    void MetaAllocator::close()
    {
        if (m_recycler_ptr) {
            // unregister all owned (i.e. associated with the same prefix) slabs from the recycler
            m_recycler_ptr->close([this](const SlabItem &slab) {
                return &slab->getPrefix() == m_prefix.get();
            });
        }
        m_realms.close();
    }
    
    std::shared_ptr<SlabAllocator> MetaAllocator::reserveNewSlab(unsigned char realm_id) {
        return m_realms[realm_id].reserveNewSlab();
    }
    
    Address MetaAllocator::getFirstAddress() const {
        return m_realms[0].getFirstAddress();
    }

    std::shared_ptr<SlabAllocator> MetaAllocator::openReservedSlab(Address address, std::size_t size) const
    {
        auto slab_id = m_slab_id_function(address);
        auto realm_id = slab_id & MetaAllocator::REALM_MASK;
        auto result = m_realms[realm_id].openReservedSlab(address, slab_id);
        assert(result->size() == size);
        return result;
    }
    
    void MetaAllocator::Realm::commit() const
    {
        // NOTE: slab manager must commit first (important!)
        // this is because it may perform modifications to the slab defs and capacity items
        m_slab_manager->commit();
        m_slab_defs.commit();
        m_capacity_items.commit();        
    }
    
    void MetaAllocator::Realm::detach() const
    {
        m_slab_defs.detach();
        m_capacity_items.detach();
        m_slab_manager->detach();
    }

    void MetaAllocator::commit() const
    {
        // NOTE: if atomic operation is in progress, the deferred free operations are not flushed
        // this is not a finalized and potentially reversible commit        
        if (!m_atomic) {
            flush();
        }
        m_realms.commit();
    }

    void MetaAllocator::detach() const {
        m_realms.detach();
    }
    
    SlabRecycler *MetaAllocator::getSlabRecyclerPtr() const {
        return m_recycler_ptr;
    }
    
    void MetaAllocator::forAllSlabs(std::function<void(const SlabAllocator &, std::uint32_t)> f) const {
        m_realms.forAllSlabs(f);        
    }
    
    void MetaAllocator::flush() const
    {
        assert(!m_atomic);
        m_realms.flush();
    }
    
    void MetaAllocator::beginAtomic()
    {        
        assert(!m_atomic);
        m_atomic = true;
        m_realms.beginAtomic();
    }
    
    void MetaAllocator::endAtomic()
    {        
        assert(m_atomic);
        m_atomic = false;
        m_realms.endAtomic();
    }
    
    void MetaAllocator::cancelAtomic()
    {
        assert(m_atomic);
        m_atomic = false;
        m_realms.cancelAtomic();
    }
    
    MetaAllocator::RealmsVector::RealmsVector(Memspace &metaspace, std::shared_ptr<Prefix> prefix, SlabRecycler *slab_recycler,
        o_meta_header &meta_header, unsigned int size, bool deferred_free)
    {
        reserve(size);
        auto slab_size = meta_header.m_slab_size;
        auto page_size = meta_header.m_page_size;
        for (unsigned int i = 0; i < size; ++i) {
            emplace_back(metaspace, prefix, slab_recycler, meta_header.m_realms[i], slab_size, 
                page_size, static_cast<unsigned char>(i), deferred_free
            );
        }
    }
    
    void MetaAllocator::RealmsVector::forAllSlabs(std::function<void(const SlabAllocator &, std::uint32_t)> f) const
    {
        for (const auto &realm: *this) {
            realm->forAllSlabs(f);
        }
    }
    
    void MetaAllocator::RealmsVector::detach() const
    {
        for (const auto &realm: *this) {
            realm.detach();
        }
    }
    
    void MetaAllocator::RealmsVector::commit() const
    {
        for (const auto &realm: *this) {
            realm.commit();
        }
    }

    void MetaAllocator::RealmsVector::beginAtomic()
    {
        for (auto &realm: *this) {
            realm->beginAtomic();
        }
    }

    void MetaAllocator::RealmsVector::endAtomic()
    {
        for (auto &realm: *this) {
            realm->endAtomic();
        }
    }

    void MetaAllocator::RealmsVector::cancelAtomic()
    {
        for (auto &realm: *this) {
            realm->cancelAtomic();
        }
    }

    void MetaAllocator::RealmsVector::close()
    {
        for (auto &realm: *this) {
            realm->close();
        }
    }

    std::uint64_t MetaAllocator::RealmsVector::getSlabMaxAddress() const
    {        
        std::uint64_t max_addr = 0;
        for (const auto &realm : *this) {
            max_addr = std::max(max_addr, realm.getSlabMaxAddress());
        }
        return max_addr;
    }

    void MetaAllocator::RealmsVector::flush() const
    {
        for (const auto &realm : *this) {
            realm->flush();
        }    
    }

    std::size_t MetaAllocator::RealmsVector::getDeferredFreeCount() const
    {
        std::size_t result = 0;
        for (const auto &realm : *this) {
            result += realm->getDeferredFreeCount();
        }
        return result;
    }
    
    std::uint32_t MetaAllocator::getSlabId(Address address) const {
        return m_slab_id_function(address);
    }
    
    std::size_t MetaAllocator::getDeferredFreeCount() const {
        return m_realms.getDeferredFreeCount();
    }

}