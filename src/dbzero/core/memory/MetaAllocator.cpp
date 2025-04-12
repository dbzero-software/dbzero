#include "MetaAllocator.hpp"
#include "OneShotAllocator.hpp"
#include "Memspace.hpp"
#include "SlabRecycler.hpp"
#include <unordered_map>
#include <dbzero/core/vspace/v_object.hpp>

namespace db0

{

    static constexpr double MIN_FILL_RATE = 0.33;

    std::size_t MetaAllocator::getSlabCount(std::size_t page_size, std::size_t slab_size) 
    {
        std::size_t max_slab_count = (std::numeric_limits<std::uint32_t>::max() - 2 * page_size) / slab_size - 1;
        // estimate the number of slabs for which the definitions can be stored on a single page
        // this is a very conservative estimate
        std::size_t slab_count_1 = (std::size_t)(MIN_FILL_RATE * (double)page_size / (double)sizeof(MetaAllocator::SlabDef));
        std::size_t slab_count_2 = (std::size_t)(MIN_FILL_RATE * (double)page_size / (double)sizeof(MetaAllocator::CapacityItem));
        return std::min(max_slab_count, std::min(slab_count_1, slab_count_2));
    }

    std::size_t align(std::size_t address, std::size_t page_size) {
        return ((address + page_size - 1) / page_size) * page_size;
    }
    
    std::function<std::uint64_t(unsigned int)> MetaAllocator::getAddressPool(std::size_t offset, std::size_t page_size, 
        std::size_t slab_size) 
    {
        auto slab_count = getSlabCount(page_size, slab_size);
        // make offset page-aligned
        offset = align(offset, page_size);
        // take the first 2 pages before a sequence of slabs
        return [offset, slab_count, page_size, slab_size](unsigned int i) -> std::uint64_t {
            assert(2 * page_size + slab_size * slab_count < std::numeric_limits<std::uint32_t>::max());
            return (std::uint64_t)(i / 2) * (2 * page_size + slab_size * slab_count) + offset + (std::uint64_t)(i % 2) * page_size;
        };
    }

    // Construct the reverse address pool function
    std::function<unsigned int(std::uint64_t)> MetaAllocator::getReverseAddressPool(std::size_t offset, std::size_t page_size, 
        std::size_t slab_size) 
    {
        auto slab_count = getSlabCount(page_size, slab_size);
        // make offset page-aligned
        offset = align(offset, page_size);
        return [offset, slab_count, page_size, slab_size](std::uint64_t addr) -> unsigned int {
            auto x = 2 * ((addr - offset) / (2 * page_size + slab_size * slab_count));
            auto d = (addr - offset) % (2 * page_size + slab_size * slab_count);
            if (d % page_size != 0) {
                THROWF(db0::InternalException) << "Invalid meta-address pool address: " << addr;
            }
            assert (d < 2 * page_size);
            return x + (d / page_size);
        };
    }
    
    std::function<std::uint32_t(std::uint64_t)> MetaAllocator::getSlabIdFunction(std::size_t offset, std::size_t page_size,
        std::size_t slab_size)
    {
        auto slab_count = getSlabCount(page_size, slab_size);
        offset = align(offset, page_size);
        auto block_size = 2 * page_size + slab_size * slab_count;
        return [offset, page_size, slab_size, slab_count, block_size](std::uint64_t address) -> std::uint32_t {
            assert(db0::isPhysicalAddress(address));
            auto block_id = (address - offset) / block_size;
            auto slab_num = (address - offset - block_id * block_size - 2 * page_size) / slab_size;
            return block_id * slab_count + slab_num;
        }; 
    }

    // Get function to translate slab id to slab address
    std::function<std::uint64_t(unsigned int)> getSlabAddressFunction(std::size_t offset, std::size_t page_size, std::size_t slab_size) 
    {
        auto slab_count = MetaAllocator::getSlabCount(page_size, slab_size);
        // make offset page-aligned
        offset = align(offset, page_size);
        auto block_size = 2 * page_size + slab_size * slab_count;
        return [offset, slab_count, page_size, slab_size, block_size](unsigned int i) -> std::uint64_t {
            auto block_id = i / slab_count;
            auto slab_num = i % slab_count;
            return offset + block_id * block_size + 2 * page_size + slab_num * slab_size;
        };
    }
    
    class SlabManager
    {
    public:    
        SlabManager(std::shared_ptr<Prefix> prefix, MetaAllocator::SlabTreeT &slab_defs, 
            MetaAllocator::CapacityTreeT &capacity_items, SlabRecycler *recycler, std::uint32_t slab_size, std::uint32_t page_size,
            std::function<std::uint64_t(unsigned int)> address_func, 
            std::function<std::uint32_t(std::uint64_t)> slab_id_func)
            : m_prefix(prefix)
            , m_slab_defs(slab_defs)
            , m_capacity_items(capacity_items)
            , m_recycler_ptr(recycler)
            , m_slab_size(slab_size)
            , m_page_size(page_size)
            , m_slab_address_func(address_func)
            , m_slab_id_func(slab_id_func)
            , m_next_slab_id(fetchNextSlabId())
        {
        }
        
        using CapacityItem = MetaAllocator::CapacityItem;        
        using SlabDef = MetaAllocator::SlabDef;
        
        struct FindResult
        {
            std::shared_ptr<SlabAllocator> m_slab;
            CapacityItem m_cap_item;

            bool operator==(std::uint32_t slab_id) const {
                return m_slab && m_cap_item.m_slab_id == slab_id;
            }

            bool operator==(const FindResult &rhs) const {
                return *this == rhs.m_cap_item.m_slab_id;
            }

            const SlabAllocator &operator*() const {
                return *m_slab;
            }
        };

        /**
         * Retrieves the active slab or returns nullptr if no active slab available
        */
        FindResult tryGetActiveSlab() {
            return m_active_slab;
        }

        /**
         * Retrieve the 1st slab to allocate a block of at least min_capacity
         * this is only a 'hint' and if the allocation is not possible, the next slab should be attempted         
        */
        FindResult findFirst(std::size_t min_capacity) 
        {
            // visit slabs starting from the largest available capacity
            auto it = m_capacity_items.cbegin();
            for (;;) {
                if (it.is_end() || it->m_remaining_capacity < min_capacity) {
                    // no existing slab has sufficient capacity
                    return {};                
                }
                
                if (m_active_slab == it->m_slab_id) {
                    // do not include active slab in find operation
                    ++it;
                    continue;
                }
                return openSlab(m_slab_address_func(it->m_slab_id));
            }
        }
        
        /**
         * Continue after findFirst
        */
        FindResult findNext(FindResult last_result, std::size_t min_capacity) 
        {
            for (;;) {
                // this is to find the next item in order
                last_result.m_cap_item.m_slab_id++;
                auto it = m_capacity_items.upper_equal_bound(last_result.m_cap_item);
                if (!it.first || it.first->m_remaining_capacity < min_capacity) {
                    return {};
                }

                if (m_active_slab == it.first->m_slab_id) {
                    // do not include active slab in find operation                    
                    continue;
                }
                return openSlab(m_slab_address_func(it.first->m_slab_id));
            }
        }
        
        unsigned int getSlabCount() const {
            return nextSlabId();
        }
        
        /**
         * Create a new, unregistered slab instance
        */
        std::pair<std::shared_ptr<SlabAllocator>, std::uint32_t> createNewSlab()
        {
            if (!m_next_slab_id) {
                m_next_slab_id = fetchNextSlabId();
            }
            auto slab_id = (*m_next_slab_id)++;
            auto address = m_slab_address_func(slab_id);
            // create the new slab
            auto capacity = SlabAllocator::formatSlab(m_prefix, address, m_slab_size, m_page_size);
            auto slab = std::make_shared<SlabAllocator>(m_prefix, address, m_slab_size, m_page_size, capacity);
            return { slab, slab_id };
        }
        
        /**
         * Create a new, registered slab instance
        */
        FindResult addNewSlab()
        {
            auto [slab, slab_id] = createNewSlab();
            auto address = m_slab_address_func(slab_id);
            CapacityItem cap_item { static_cast<std::uint32_t>(slab->getRemainingCapacity()), slab_id };            
            // register with slab defs
            m_slab_defs.emplace(slab_id, static_cast<std::uint32_t>(cap_item.m_remaining_capacity));
            // register with capacity items
            m_capacity_items.insert(cap_item);
            // add to cache
            auto cache_item = std::make_shared<CacheItem>(slab, cap_item);
            m_slabs.emplace(address, cache_item);
            // capture remaining capacity before instance is closed
            slab->setOnCloseHandler([cache_item](const SlabAllocator &alloc) {
                cache_item->m_final_remaining_capacity = alloc.getRemainingCapacity();
            });
            
            // append with the recycler
            if (m_recycler_ptr) {
                m_recycler_ptr->append(slab);
            }

            // make the new slab active
            m_active_slab = { slab, cap_item };
            return m_active_slab;
        }

        std::uint32_t getRemainingCapacity(std::uint32_t slab_id) const
        {
            // look up with the cache first
            auto address = m_slab_address_func(slab_id);
            auto it = m_slabs.find(address);
            if (it != m_slabs.end()) {
                auto slab = it->second->m_slab.lock();
                if (slab) {                    
                    return slab->getRemainingCapacity();
                }
            }

            // look up with the slab defs next
            auto slab_def_ptr = m_slab_defs.find_equal(slab_id);
            if (!slab_def_ptr.first) {
                THROWF(db0::InternalException) << "Slab definition not found.";
            }
            return slab_def_ptr.first->m_remaining_capacity;
        }

        void close()
        {
            m_active_slab = {};
            m_reserved_slabs.clear();
            for (auto it = m_slabs.begin(); it != m_slabs.end();) {
                it = unregisterSlab(it);
            }
        }

        /**
         * Find existing slab by ID
        */
        FindResult find(std::uint32_t slab_id) 
        {
            if (slab_id >= nextSlabId()) {
                THROWF(db0::InputException) << "Slab " << slab_id << " does not exist";
            }
            if (m_active_slab == slab_id) {
                return m_active_slab;
            }
            // look up with the cache first
            auto address = m_slab_address_func(slab_id);
            auto it = m_slabs.find(address);
            if (it != m_slabs.end()) {
                auto slab = it->second->m_slab.lock();
                if (slab) {
                    return { slab, it->second->m_cap_item };
                }
            }

            return openSlab(address);
        }
        
        /**
         * Erase if 'slab' is the last slab
        */
        void erase(const FindResult &slab) {
            erase(slab, true);
        }
        
        bool empty() const {
            return nextSlabId() == 0;
        }

        std::shared_ptr<SlabAllocator> reserveNewSlab()
        {
            auto [slab, slab_id] = createNewSlab();
            // internally register the slab with capacity = 0
            CapacityItem cap_item { 0, slab_id };
            // register with slab defs
            m_slab_defs.emplace(slab_id, static_cast<std::uint32_t>(cap_item.m_remaining_capacity));
            // register with capacity items
            m_capacity_items.insert(cap_item);
            return slab;
        }
        
        std::shared_ptr<SlabAllocator> openExistingSlab(const SlabDef &slab_def)
        {
            if (slab_def.m_slab_id >= nextSlabId()) {
                THROWF(db0::InputException) << "Slab " << slab_def.m_slab_id << " does not exist";
            }            
            auto address = m_slab_address_func(slab_def.m_slab_id);
            // look up with the cache first
            auto it = m_slabs.find(address);
            if (it != m_slabs.end()) {
                auto slab = it->second->m_slab.lock();
                if (slab) {
                    return slab;
                }
            }
            // pull through cache
            return openSlab(slab_def).m_slab;
        }

        /**
         * Open existing slab which has been previously reserved
        */
        std::shared_ptr<SlabAllocator> openReservedSlab(std::uint64_t address)
        {
            auto slab_id = m_slab_id_func(address);
            if (slab_id >= nextSlabId()) {
                THROWF(db0::InputException) << "Slab " << slab_id << " does not exist";
            }            

            // look up with the cache first
            auto it = m_slabs.find(address);
            if (it != m_slabs.end()) {
                auto slab = it->second->m_slab.lock();
                if (slab) {
                    return slab;
                }
            }

            // retrieve slab definition
            auto slab_def_ptr = m_slab_defs.find_equal(slab_id);
            if (!slab_def_ptr.first) {
                THROWF(db0::InternalException) << "Slab definition not found: " << slab_id;
            }
            
            // pull through cache
            auto result = openSlab(*slab_def_ptr.first).m_slab;
            // and add for non-expiry cache
            m_reserved_slabs.push_back(result);
            return result;
        }

        std::uint64_t getFirstAddress() const {
            return m_slab_address_func(0) + SlabAllocator::getFirstAddress();
        }

        void commit() const 
        {
            for (auto &it : m_slabs) {
                it.second->commit();
            }
        }
        
        void detach() const
        {
            // invalidate cached variable
            m_next_slab_id = {};
            for (auto &it : m_slabs) {
                it.second->detach();
            }
        }
        
        std::uint32_t nextSlabId() const 
        {
            if (!m_next_slab_id) {
                m_next_slab_id = fetchNextSlabId();
            }
            return *m_next_slab_id;
        }

    private:

        struct CacheItem
        {
            std::weak_ptr<SlabAllocator> m_slab;
            CapacityItem m_cap_item;
            // the slab's remaining capacity refreshed when SlabAllocator gets destroyed
            std::uint32_t m_final_remaining_capacity = 0;

            CacheItem(std::weak_ptr<SlabAllocator> slab, CapacityItem cap)
                : m_slab(slab)
                , m_cap_item(cap)
            {
            }

            void commit() const 
            {
                if (auto slab = m_slab.lock()) {
                    slab->commit();
                }
            }

            void detach() const 
            {
                if (auto slab = m_slab.lock()) {
                    slab->detach();
                }
            }
        };
        
        using CacheIterator = std::unordered_map<std::uint64_t, std::shared_ptr<CacheItem> >::iterator;

        std::shared_ptr<Prefix> m_prefix;
        MetaAllocator::SlabTreeT &m_slab_defs;
        MetaAllocator::CapacityTreeT &m_capacity_items;
        SlabRecycler *m_recycler_ptr = nullptr;
        const std::uint32_t m_slab_size;
        const std::uint32_t m_page_size;
        // slab cache by address
        std::unordered_map<std::uint64_t, std::shared_ptr<CacheItem> > m_slabs;
        std::vector<std::shared_ptr<SlabAllocator> > m_reserved_slabs;
        FindResult m_active_slab;
        // address by allocation ID (from the algo-allocator)
        std::function<std::uint64_t(unsigned int)> m_slab_address_func;
        std::function<std::uint32_t(std::uint64_t)> m_slab_id_func;
        mutable std::optional<std::uint32_t> m_next_slab_id;
        
        CacheIterator unregisterSlab(CacheIterator it)
        {
            auto cache_item = it->second;
            if (!cache_item->m_slab.expired()) {
                THROWF(db0::InternalException) 
                    << "Slab " << static_cast<std::uint32_t>(cache_item->m_cap_item.m_slab_id) << " is not closed";
            }
            
            auto &item = *cache_item;
            // if the remaining capacity has hanged, reflect this with backend
            if (item.m_final_remaining_capacity != item.m_cap_item.m_remaining_capacity) {
                auto slab_id = item.m_cap_item.m_slab_id;
                auto it = m_capacity_items.find_equal(item.m_cap_item);
                // register under a modified key
                m_capacity_items.erase(it);
                m_capacity_items.emplace(item.m_final_remaining_capacity, slab_id);
                // and update with the slab defs
                auto slab_def_ptr = m_slab_defs.find_equal(slab_id);
                m_slab_defs.modify(slab_def_ptr)->m_remaining_capacity = item.m_final_remaining_capacity;
            }
            return m_slabs.erase(it);
        }
        
        FindResult openSlab(std::uint64_t address)
        {
            auto it = m_slabs.find(address);
            if (it != m_slabs.end()) {
                auto result = it->second->m_slab.lock();
                if (result) {
                    return { result, it->second->m_cap_item };
                }
                // unregister expired slab from cache
                unregisterSlab(it);
            }

            auto slab_id = m_slab_id_func(address);
            // retrieve slab definition
            auto slab_def_ptr = m_slab_defs.find_equal(slab_id);
            if (!slab_def_ptr.first) {
                THROWF(db0::InternalException) << "Slab definition not found: " << slab_id;
            }

            // make the new slab active
            m_active_slab = openSlab(*slab_def_ptr.first);
            return m_active_slab;
        }

        // open slab by definition and add to cache
        FindResult openSlab(const SlabDef &def)
        {
            auto cap_item = CapacityItem(def.m_remaining_capacity, def.m_slab_id);
            auto addr = m_slab_address_func(def.m_slab_id);
            auto slab = std::make_shared<SlabAllocator>(m_prefix, addr, m_slab_size, m_page_size, def.m_remaining_capacity);
            // add to cache (it's safe to reference item from the unordered_map)
            auto cache_item = std::make_shared<CacheItem>(slab, cap_item);
            m_slabs.emplace(addr, cache_item).first->second;
            // capture remaining capacity before instance is closed
            slab->setOnCloseHandler([cache_item](const SlabAllocator &alloc) {
                cache_item->m_final_remaining_capacity = alloc.getRemainingCapacity();
            });
            
            // append with the recycler
            if (m_recycler_ptr) {
                m_recycler_ptr->append(slab);
            }

            return { slab, cap_item };
        }

        void erase(const FindResult &slab, bool cleanup)
        {
            if (slab.m_cap_item.m_slab_id != nextSlabId() - 1) {
                return;
            }

            auto addr = m_slab_address_func(slab.m_cap_item.m_slab_id);
            // unregister from cache
            auto it = m_slabs.find(addr);
            if (it != m_slabs.end()) {
                m_slabs.erase(it);
            }
            // unregister from recycler
            if (m_recycler_ptr) {
                m_recycler_ptr->closeOne([&slab](const SlabAllocator &s) {
                    return slab.m_slab.get() == &s;
                });
            }
            // unregister if active
            if (m_active_slab == slab) {
                m_active_slab = {};
            }
            // unregister from slab defs
            if (!m_slab_defs.erase_equal(slab.m_cap_item.m_slab_id).first) {
                THROWF(db0::InternalException) << "Slab definition not found.";
            }
            // unregister from capacity items
            if (!m_capacity_items.erase_equal(slab.m_cap_item).first) {
                THROWF(db0::InternalException) << "Capacity item not found.";
            }
            if (!m_next_slab_id) {
                m_next_slab_id = fetchNextSlabId();
            }
            --(*m_next_slab_id);
            // try removing other empty slabs if such exist
            if (cleanup) {
                while (!empty()) {
                    auto slab = openSlab(m_slab_address_func(nextSlabId() - 1));
                    if (!slab.m_slab->empty()) {
                        break;
                    }
                    erase(slab, false);                    
                }
            }            
        }

        std::uint32_t fetchNextSlabId() const
        {
            // determine the max slab id
            auto it = m_slab_defs.find_max();
            if (it.first) {
                return it.first->m_slab_id + 1;
            } else {
                return 0;
            }
        }

    };
    
    MetaAllocator::MetaAllocator(std::shared_ptr<Prefix> prefix, SlabRecycler *recycler, bool deferred_free)
        : m_prefix(prefix)
        , m_header(getMetaHeader(prefix))
        , m_algo_allocator(
            getAddressPool(o_meta_header::sizeOf(), m_header.m_page_size, m_header.m_slab_size),
            getReverseAddressPool(o_meta_header::sizeOf(), m_header.m_page_size, m_header.m_slab_size),
            m_header.m_page_size
        )
        , m_metaspace(createMetaspace())
        , m_slab_defs(m_metaspace.myPtr(m_header.m_slab_defs_ptr), m_header.m_page_size)
        , m_capacity_items(m_metaspace.myPtr(m_header.m_capacity_items_ptr), m_header.m_page_size)
        , m_slab_manager(std::make_unique<SlabManager>(prefix, m_slab_defs, m_capacity_items, recycler,
            static_cast<std::uint32_t>(m_header.m_slab_size),
            static_cast<std::uint32_t>(m_header.m_page_size),
            getSlabAddressFunction(o_meta_header::sizeOf(), m_header.m_page_size, m_header.m_slab_size),
            getSlabIdFunction(o_meta_header::sizeOf(), m_header.m_page_size, m_header.m_slab_size)
            ))
        , m_recycler_ptr(recycler)
        , m_deferred_free(deferred_free)
        , m_slab_id_function(getSlabIdFunction(o_meta_header::sizeOf(), m_header.m_page_size, m_header.m_slab_size))
    {
        // iterate the slab-defs / capacity items trees to determine the max address assigned within the algo-space
        std::uint64_t max_addr = 0;
        for (auto it = m_slab_defs.cbegin_nodes(), end = m_slab_defs.cend_nodes(); it != end; ++it) {
            max_addr = std::max(max_addr, it.getAddress());
        }
        for (auto it = m_capacity_items.cbegin_nodes(), end = m_capacity_items.cend_nodes(); it != end; ++it) {
            max_addr = std::max(max_addr, it.getAddress());
        }
        // pass this address to the meta-space
        if (max_addr > 0) {
            m_algo_allocator.setMaxAddress(max_addr);
        }
    }
    
    MetaAllocator::~MetaAllocator()
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
        // create the meta-header and the address 0x0
        OneShotAllocator one_shot(0, o_meta_header::sizeOf());
        Memspace memspace(Memspace::tag_from_reference(), prefix, one_shot);
        v_object<o_meta_header> meta_header(memspace, page_size, slab_size, 0, 0);
        auto offset = o_meta_header::sizeOf();
        // Construct the meta-space for the slab tree
        AlgoAllocator algo_allocator(
                getAddressPool(offset, page_size, slab_size), 
                getReverseAddressPool(offset, page_size, slab_size), 
                page_size);
        Memspace meta_space(Memspace::tag_from_reference(), prefix, algo_allocator);

        // Create the empty slab-defs and capacity items trees on the meta-space
        SlabTreeT slab_defs(meta_space, page_size);
        CapacityTreeT capacity_items(meta_space, page_size);
        
        // and put their addresses in the header...
        meta_header.modify().m_slab_defs_ptr = slab_defs.getAddress();
        meta_header.modify().m_capacity_items_ptr = capacity_items.getAddress();
    }
    
    o_meta_header MetaAllocator::getMetaHeader(std::shared_ptr<Prefix> prefix)
    {
        // meta-header is located at the fixed address 0x0
        OneShotAllocator one_shot(0, o_meta_header::sizeOf());
        Memspace memspace(Memspace::tag_from_reference(), prefix, one_shot);
        v_object<o_meta_header> meta_header(memspace.myPtr(0));
        return meta_header.const_ref();
    }
    
    std::optional<std::uint64_t> MetaAllocator::tryAlloc(std::size_t size, std::uint32_t slot_num,
        bool aligned, bool unique)
    {
        assert(slot_num == 0);
        assert(size > 0);
        // try allocating from the active slab first
        auto slab = m_slab_manager->tryGetActiveSlab();
        bool is_first = true;
        bool is_new = false;
        for (;;) {
            if (slab.m_slab) {
                for (;;) {
                    auto addr = slab.m_slab->tryAlloc(size, 0, aligned, false);
                    if (!addr) {
                        break;
                    }

                    if (!unique || slab.m_slab->makeAddressUnique(*addr)) {
                        // NOTE: the returned address is logical
                        return addr;
                    }
                                     
                    // unable to make the address unique, schedule for deferred free and try again
                    // NOTE: the allocation is lost
                    deferredFree(*addr);
                }
                if (size > slab.m_slab->getMaxAllocSize()) {
                    THROWF(db0::InternalException) 
                        << "Requested allocation size " << size << " is larger than the slab size " << slab.m_slab->getMaxAllocSize();
                }
                if (is_new) {
                    THROWF(db0::InternalException) << "Slab is new but cannot allocate " << size;
                }
            }
            if (is_first) {
                slab = m_slab_manager->findFirst(size);
                is_first = false;
            } else {
                slab = m_slab_manager->findNext(slab, size); 
            }
            if (!slab.m_slab) {
                slab = m_slab_manager->addNewSlab();
                is_new = true;
            }
        }
    }
    
    void MetaAllocator::free(std::uint64_t address)
    {
        address = db0::getPhysicalAddress(address);
        assert(m_deferred_free_ops.find(address) == m_deferred_free_ops.end());
        if (m_deferred_free) {
            deferredFree(address);
        } else {
            _free(address);
        }
    }

    void MetaAllocator::deferredFree(std::uint64_t address)
    {
        assert(db0::isPhysicalAddress(address));
        if (m_atomic) {
            m_atomic_deferred_free_ops.push_back(address);
        } else {
            m_deferred_free_ops.insert(address);
        }
    }
    
    void MetaAllocator::_free(std::uint64_t address)
    {
        assert(db0::isPhysicalAddress(address));
        auto slab_id = m_slab_id_function(address);
        auto slab = m_slab_manager->find(slab_id);
        slab.m_slab->free(address);
        if (slab.m_slab->empty()) {
            // erase or mark as erased
            m_slab_manager->erase(slab);
        }
    }
    
    std::size_t MetaAllocator::getAllocSize(std::uint64_t address) const
    {        
        address = db0::getPhysicalAddress(address);
        if (m_deferred_free_ops.find(address) != m_deferred_free_ops.end()) {
            THROWF(db0::InputException) << "Address " << address << " not found (pending deferred free)";
        }
        auto slab_id = m_slab_id_function(address);
        return m_slab_manager->find(slab_id).m_slab->getAllocSize(address);        
    }

    bool MetaAllocator::isAllocated(std::uint64_t address) const
    {        
        address = db0::getPhysicalAddress(address);
        if (m_deferred_free_ops.find(address) != m_deferred_free_ops.end()) {
            return false;
        }
        auto slab_id = m_slab_id_function(address);
        return m_slab_manager->find(slab_id).m_slab->isAllocated(address);
    }

    std::uint32_t MetaAllocator::getSlabId(std::uint64_t address) const {
        return m_slab_id_function(address);
    }

    unsigned int MetaAllocator::getSlabCount() const {
        return m_slab_manager->getSlabCount();
    }
    
    std::uint32_t MetaAllocator::getRemainingCapacity(std::uint32_t slab_id) const {
        return m_slab_manager->getRemainingCapacity(slab_id);
    }

    void MetaAllocator::close()
    {
        if (m_recycler_ptr) {
            // unregister all owned (i.e. associated with the same prefix) slabs from the recycler
            m_recycler_ptr->close([this](const SlabAllocator &slab) {
                return &slab.getPrefix() == m_prefix.get();
            });
        }
        m_slab_manager->close();
    }

    std::shared_ptr<SlabAllocator> MetaAllocator::reserveNewSlab() {
        return m_slab_manager->reserveNewSlab();
    }
    
    std::uint64_t MetaAllocator::getFirstAddress() const {
        return m_slab_manager->getFirstAddress();
    }

    std::shared_ptr<SlabAllocator> MetaAllocator::openReservedSlab(std::uint64_t address, std::size_t size) const
    {
        auto result = m_slab_manager->openReservedSlab(address);
        assert(result->size() == size);
        return result;
    }
    
    void MetaAllocator::commit() const
    {
        // NOTE: if atomic operation is in progress, the deferred free operations are not flushed
        // this is not a finalized and potentially reversible commit        
        if (!m_atomic) {
            flush();
        }
        m_slab_defs.commit();
        m_capacity_items.commit();
        m_slab_manager->commit();
    }

    void MetaAllocator::detach() const
    { 
        m_slab_defs.detach();
        m_capacity_items.detach();
        m_slab_manager->detach();
    }
    
    SlabRecycler *MetaAllocator::getSlabRecyclerPtr() const {
        return m_recycler_ptr;
    }
    
    void MetaAllocator::forAllSlabs(std::function<void(const SlabAllocator &, std::uint32_t)> f) const
    {
        auto it = m_slab_defs.cbegin();
        for (;!it.is_end();++it) {
            auto slab = m_slab_manager->openExistingSlab(*it);
            f(*slab, it->m_slab_id);
        }
    }

    void MetaAllocator::flush() const
    {
        assert(!m_atomic);
        assert(m_atomic_deferred_free_ops.empty());
        // perform the deferred free operations
        if (!m_deferred_free_ops.empty()) {
            for (auto addr : m_deferred_free_ops) {
                const_cast<MetaAllocator&>(*this)._free(addr);
            }
            m_deferred_free_ops.clear();
        }
    }
    
    std::size_t MetaAllocator::getDeferredFreeCount() const {
        return m_deferred_free_ops.size();
    }

    void MetaAllocator::beginAtomic()
    {
        assert(!m_atomic);
        m_atomic = true;
    }

    void MetaAllocator::endAtomic()
    {
        assert(m_atomic);
        m_atomic = false;
        // merge atomic deferred free operations
        if (!m_atomic_deferred_free_ops.empty()) {
            for (auto addr : m_atomic_deferred_free_ops) {
                m_deferred_free_ops.insert(addr);
            }
            m_atomic_deferred_free_ops.clear();
        }
    }

    void MetaAllocator::cancelAtomic()
    {
        assert(m_atomic);
        m_atomic = false;
        // rollback atomic deferred free operations
        m_atomic_deferred_free_ops.clear(); 
    }
    
}

namespace std 

{
    ostream &operator<<(ostream &os, const db0::MetaAllocator::CapacityItem &item) {
        os << "CapacityItem(capacity=" << item.m_remaining_capacity << ", slab=" << item.m_slab_id << ")";
        return os;
    }

    ostream &operator<<(ostream &os, const db0::MetaAllocator::SlabDef &def) {
        os << "SlabDef(slab=" << def.m_slab_id << ", capacity=" << def.m_remaining_capacity << ")";
        return os;
    }

}