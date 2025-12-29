// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "Fixture.hpp"
#include <dbzero/core/memory/MetaAllocator.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/utils/uuid.hpp>
#include <dbzero/core/utils/ProcessTimer.hpp>
#include "GC0.hpp"
#include "Workspace.hpp"
#include "WorkspaceView.hpp"
#include "PrefixName.hpp"

namespace db0

{
        
    std::uint32_t slot_index(std::uint32_t slot_num) {
        return slot_num - 1;
    }

    o_fixture::o_fixture()
        : m_UUID(db0::make_UUID())
    {
    }
    
    Fixture::Fixture(Workspace &workspace, std::shared_ptr<Prefix> prefix, std::shared_ptr<MetaAllocator> meta, int locked_sections)
        : Fixture(workspace, workspace.getSharedObjectList(), prefix, meta, locked_sections)
    {        
    }
    
    Fixture::Fixture(Snapshot &snapshot, FixedObjectList &shared_object_list, std::shared_ptr<Prefix> prefix, std::shared_ptr<MetaAllocator> meta,
        int locked_sections)
        : Memspace(prefix, std::make_shared<SlotAllocator>(meta), getUUID(prefix, *meta))
        , m_access_type(prefix->getAccessType())
        , m_snapshot(snapshot)
        , m_lang_cache(*this, snapshot.getLangCache())
        , m_slot_allocator(reinterpret_cast<SlotAllocator&>(*m_allocator))
        , m_meta_allocator(reinterpret_cast<MetaAllocator&>(*m_slot_allocator.getAllocator()))        
        , m_UUID(*m_derived_UUID)
        , m_string_pool(openLimitedStringPool(*this, *meta))
        , m_object_catalogue(openObjectCatalogue(*meta))
        , m_v_object_cache(*this, shared_object_list)
        , m_mutation_log(locked_sections)
    {
        // set-up slots with the allocator
        m_slot_allocator.setSlot(TYPE_SLOT_NUM, openSlot(*this, *meta, TYPE_SLOT_NUM));
    }
    
    Fixture::~Fixture()
    {
    }
    
    StringPoolT Fixture::openLimitedStringPool(Memspace &memspace, MetaAllocator &meta)
    {
        using v_fixture = v_object<o_fixture>;
        
        // read fixture configuration from under the 1st address
        v_fixture fixture(this->myPtr(meta.getFirstAddress()));
        // open the lsp-slot for the exclusive use of the limited string pool
        auto lsp_slot = openSlot(meta, fixture, LSP_SLOT_NUM);
        return StringPoolT(Memspace(this->getPrefixPtr(), lsp_slot), memspace.myPtr(fixture->m_string_pool_ptr.getAddress()));
    }
    
    std::shared_ptr<SlabAllocator> Fixture::openSlot(Memspace &memspace, MetaAllocator &meta, std::uint32_t slot_num)
    {
        using v_fixture = v_object<o_fixture>;
        v_fixture fixture(this->myPtr(meta.getFirstAddress()));
        return openSlot(meta, fixture, slot_num);
    }
    
    std::shared_ptr<SlabAllocator> Fixture::openSlot(MetaAllocator &meta, const v_object<o_fixture> &fixture, std::uint32_t slot_num) 
    {
        auto index = slot_index(slot_num);
        return meta.openReservedSlab(fixture->m_slots[index].m_address, fixture->m_slots[index].m_size);
    }
    
    db0::ObjectCatalogue Fixture::openObjectCatalogue(MetaAllocator &meta)
    {
        using v_fixture = v_object<o_fixture>;
        
        // read fixture configuration from under the 1st address
        v_fixture fixture(this->myPtr(meta.getFirstAddress()));
        return { myPtr(fixture->m_object_catalogue_address) };
    }
    
    std::uint64_t Fixture::getUUID(MetaAllocator &meta)
    {
        using v_fixture = v_object<o_fixture>;
        
        // read fixture configuration from under the 1st address
        v_fixture fx(this->myPtr(meta.getFirstAddress()));
        return fx->m_UUID;
    }
    
    std::uint64_t Fixture::getUUID(std::shared_ptr<Prefix> prefix, MetaAllocator &meta)
    {
        using v_fixture = v_object<o_fixture>;
        
        Memspace memspace(Memspace::tag_from_reference{}, prefix, meta);
        v_fixture fx(memspace.myPtr(meta.getFirstAddress()));
        return fx->m_UUID;
    }
    
    void Fixture::formatFixture(Memspace memspace, MetaAllocator &meta)
    {
        using v_fixture = v_object<o_fixture>;

        // create v_fixture as the 1st object in the memspace
        // this also generates the random UUID
        v_fixture fixture(memspace);
        // address must be the 1st address
        if (fixture.getAddress() != meta.getFirstAddress()) {
            THROWF(db0::InternalException) << "Cannot initialize new fixture because the memspace is not empty";
        }
        
        // reserve a single slab for the limited string pool (i.e. slot-0)
        {
            auto lsp_slot = meta.reserveNewSlab();
            auto index = slot_index(LSP_SLOT_NUM);
            fixture.modify().m_slots[index] = { lsp_slot->getAddress(), lsp_slot->getSlabSize() };
            // create the string pool object
            StringPoolT string_pool(Memspace(memspace.getPrefixPtr(), lsp_slot), memspace);
            fixture.modify().m_string_pool_ptr = string_pool;
        }

        // create type slot (with the purpose to store Class and Enum objects)
        {
            auto type_slot = meta.reserveNewSlab();
            auto index = slot_index(TYPE_SLOT_NUM);
            fixture.modify().m_slots[index] = { type_slot->getAddress(), type_slot->getSlabSize() };
        }

        // create the Object Catalogue
        ObjectCatalogue object_catalogue(memspace);
        fixture.modify().m_object_catalogue_address = object_catalogue.getAddress();
    }

    void Fixture::addCloseHandler(std::function<void(bool)> f) {
        m_close_handlers.push_back(f);
    }
    
    void Fixture::addDetachHandler(std::function<void()> f) {
        m_detach_handlers.push_back(f);
    }

    void Fixture::addRollbackHandler(std::function<void()> f) {
        m_rollback_handlers.push_back(f);
    }

    void Fixture::addFlushHandler(std::function<void()> f) {
        m_flush_handlers.push_back(f);
    }
    
    std::shared_ptr<MutationLog> Fixture::addMutationHandler()
    {
        auto result = std::make_shared<MutationLog>(m_mutation_log.size());
        m_mutation_handlers.push_back(result);
        return result;
    }
    
    void Fixture::rollback()
    {
        for (auto &handler: m_rollback_handlers) {
            handler();
        }
    }
    
    void Fixture::flush()
    {
        // NOTE: prevent cleanups during commit to avoid unwanted side-effects
        if (m_commit_pending) {
            return;
        }

        for (auto &handler: m_flush_handlers) {
            handler();
        }        
    }
    
    void Fixture::close(bool as_defunct, ProcessTimer *timer_ptr)
    {
        std::unique_ptr<ProcessTimer> timer;
        if (timer_ptr) {
            timer = std::make_unique<ProcessTimer>("Fixture::close", timer_ptr);
        }
        
        // clear cache to destroy object instances supported by the cache
        // this has to be done before commit (to not commit unrefereced objects)        
        m_lang_cache.clear(true, as_defunct);
        
        // auto-commit before closing
        if (m_access_type == AccessType::READ_WRITE) {
            // prevents commit on a closed fixture
            std::unique_lock<std::mutex> lock(m_close_mutex);
            if (!Memspace::isClosed()) {
                // flush to prepare objects which require it (e.g. Index) for commit
                // NOTE: flush must NOT lock the fixture's shared mutex
                if (m_gc0_ptr) {
                    getGC0().flushAllOf(Memspace::getForFlush());
                }
                
                // clear lang cache again since flush might've released some Python instances                
                m_lang_cache.clear(true);

                // lock for exclusive access
                std::unique_lock<std::shared_mutex> lock(m_commit_mutex);
                tryCommit(lock, timer.get());
                auto callbacks = collectStateReachedCallbacks();
                lock.unlock();
                executeStateReachedCallbacks(callbacks);
            }
        }
        
        for (auto &close: m_close_handlers) {
            close(false);
        }        
        m_string_pool.close();
        Memspace::close(timer.get());
    }
    
    bool Fixture::refresh(ProcessTimer *timer_ptr)
    {
        std::unique_ptr<ProcessTimer> timer;
        if (timer_ptr) {
            timer = std::make_unique<ProcessTimer>("Fixture::refresh", timer_ptr);
        }
        
        assert(getAccessType() == AccessType::READ_ONLY && "Refresh only makes sense for read-only fixtures");
        if (!Memspace::beginRefresh()) {
            return false;
        }
        
        if (m_gc0_ptr) {
            // detach all active ObjectBase instances so that they can be refreshed
            m_gc0_ptr->detachAll();
            // detach GC0 instance itself
            getGC0().detach();
        }
        
        // detach owned resources
        for (auto &detach: m_detach_handlers) {
            detach();
        }
        
        m_v_object_cache.detach();
        m_string_pool.detach();
        m_object_catalogue.detach();        
        Memspace::detach();
        Memspace::completeRefresh();

        return true;
    }

    void Fixture::onUpdated()
    {
        // collect locked-section mutations            
        m_mutation_log.onDirty();
        m_updated = true;
    }
    
    Fixture::StateReachedCallbackList Fixture::onRefresh()
    {
        std::unique_lock<std::mutex> lock(m_close_mutex);
        if (Memspace::isClosed()) {
            return {};
        }

        {
            std::unique_lock<std::shared_mutex> lock(m_commit_mutex);
            refresh();
            return collectStateReachedCallbacks();      
        }
    }

    bool Fixture::refreshIfUpdated()
    {
        // only refresh read-only fixtures        
        if (getAccessType() == AccessType::READ_ONLY && m_updated) {
            return refresh();
        }
        return false;
    }
    
    db0::swine_ptr<Fixture> Fixture::getSnapshot(Snapshot &workspace_view, std::optional<std::uint64_t> state_num) const
    {
        auto px_snapshot = m_prefix->getSnapshot(state_num);
        auto allocator_snapshot = std::make_shared<MetaAllocator>(px_snapshot, m_meta_allocator.getSlabRecyclerPtr());        
        return db0::make_swine<Fixture>(
            workspace_view, m_v_object_cache.getSharedObjectList(), px_snapshot, allocator_snapshot
        );
    }
    
    bool Fixture::commit()
    {
        std::unique_ptr<ProcessTimer> process_timer;
        // process_timer = std::make_unique<ProcessTimer>("Fixture::commit");
        assert(getPrefixPtr());
        // flush to prepare objects which require it (e.g. Index) for commit
        // NOTE: flush must NOT lock the fixture's shared mutex
        // NOTE: flush may release some of the Python instances
        if (m_gc0_ptr) {
            getGC0().flushAllOf(Memspace::getForFlush(), process_timer.get());
        }
        
        // Flush using registered flush handlers
        {        
            std::unique_ptr<ProcessTimer> flush_timer;
            if (process_timer) {
                flush_timer = std::make_unique<ProcessTimer>("Fixture::commit:flush_handlers", process_timer.get());
            }
            for (auto &handler: m_flush_handlers) {
                handler();
            }
        }
        
        // Clear Python-side expired instances from cache so that they're not persisted
        m_lang_cache.clear(true);
        std::unique_lock<std::shared_mutex> lock(m_commit_mutex);
        bool result = tryCommit(lock, process_timer.get());
        m_updated = false;
        auto callbacks = collectStateReachedCallbacks();
        lock.unlock();
        executeStateReachedCallbacks(callbacks);
        return result;
    }
    
    bool Fixture::tryCommit(std::unique_lock<std::shared_mutex> &lock, ProcessTimer *parent_timer)
    {
        bool result = false;
        m_commit_pending = true;
        try {
            std::unique_ptr<ProcessTimer> timer;
            if (parent_timer) {
                timer = std::make_unique<ProcessTimer>("Fixture::tryCommit", parent_timer);
            }
            auto prefix_ptr = getPrefixPtr();
            // prefix may not exist if fixture has already been closed
            if (!prefix_ptr) {
                return result;
            }
            
            std::unique_ptr<GC0::CommitContext> ctx = m_gc0_ptr ? m_gc0_ptr->beginCommit() : nullptr;
            // NOTE: close handlers perform internal buffers flush (e.g. TagIndex)
            // which may result in modifications (e.g. incRef)
            // it's therefore important to perform this action before GC0::commitAll (which commits finalized objects)
            for (auto &commit: m_close_handlers) {
                commit(true);
            }
            
            // Commit modified only (to avoid scan over all objects)
            if (ctx) {
                ctx->commitAllOf(Memspace::getModified(), timer.get());
                ctx = nullptr;
            }
            
            m_string_pool.commit();
            m_object_catalogue.commit();
            m_v_object_cache.commit();
            result = Memspace::commit(timer.get());
        } catch (...) {
            m_commit_pending = false;
            throw;
        }
        m_commit_pending = false;
        return result;
    }
    
    Fixture::StateReachedCallbackList Fixture::onAutoCommit()
    {
        if (m_updated) {
            // prevents commit on a closed fixture
            std::unique_lock<std::mutex> lock(m_close_mutex);
            if (Memspace::isClosed()) {
                // since it's closed we'll not be able to commit any updates anyway
                m_updated = false;
                return {};
            }

            assert(!Memspace::isClosed());
            // flush to prepare objects which require it (e.g. Index) for commit
            // NOTE: flush must NOT lock the fixture's shared mutex
            if (m_gc0_ptr) {
                getGC0().flushAllOf(Memspace::getForFlush());
            }
            
            // Flush using registered flush handlers
            for (auto &handler: m_flush_handlers) {
                handler();
            }              
            m_lang_cache.clear(true);
            // lock for exclusive access
            {
                std::unique_lock<std::shared_mutex> lock(m_commit_mutex);
                tryCommit(lock);
                m_updated = false;
                return collectStateReachedCallbacks();      
            }
        }
        return {};
    }
    
    db0::GC0 &Fixture::createGC0(db0::swine_ptr<Fixture> &fixture)
    {
        assert(!m_gc0_ptr);
        m_gc0_ptr = &addResource<db0::GC0>(fixture);
        return *m_gc0_ptr;
    }
    
    db0::GC0 &Fixture::createGC0(db0::swine_ptr<Fixture> &fixture, Address address, bool read_only)
    {
        assert(!m_gc0_ptr);
        m_gc0_ptr = &addResource<db0::GC0>(fixture, address, read_only);
        return *m_gc0_ptr;
    }
    
    const Snapshot &Fixture::getWorkspace() const {
        return m_snapshot;
    }
    
    Snapshot &Fixture::getWorkspace() {
        return m_snapshot;
    }
    
    std::uint64_t Fixture::makeRelative(Address address, std::uint32_t slot_num) const {
        return m_slot_allocator.getSlot(slot_num).makeRelative(address);
    }
    
    Address Fixture::makeAbsolute(std::uint64_t offset, std::uint32_t slot_num) const {
        return m_slot_allocator.getSlot(slot_num).makeAbsolute(offset);
    }
    
    bool Fixture::operator==(const Fixture &other) const {
        return m_UUID == other.m_UUID;
    }
    
    bool Fixture::operator!=(const Fixture &other) const {
        return m_UUID != other.m_UUID;
    }
    
    void Fixture::preAtomic()
    {
        getGC0().flushAllOf(Memspace::getForFlush());
        m_maybe_need_flush.clear();
        for (auto &commit: m_close_handlers) {
            commit(true);
        }
    }
    
    void Fixture::beginAtomic(AtomicContext *context)
    {
        assert(!m_atomic_context_ptr);
        m_atomic_context_ptr = context;
        m_meta_allocator.beginAtomic();        
        getGC0().beginAtomic();
        m_string_pool.commit();
        m_object_catalogue.commit();
        m_v_object_cache.beginAtomic();
        Memspace::beginAtomic();
    }
    
    void Fixture::detach()
    {
        // commit and then detach owned resources (potentially modified in atomic context)
        for (auto &commit: m_close_handlers) {
            commit(true);
        }
        for (auto &detach: m_detach_handlers) {
            detach();
        }
        
        // detach GC0 instance itself
        if (m_gc0_ptr) {
            getGC0().detach();
        }
        
        m_object_catalogue.detach();
        m_v_object_cache.detach();
        m_string_pool.detach();
        m_object_catalogue.detach();
        Memspace::detach();
    }
    
    void Fixture::endAtomic()
    {        
        assert(m_atomic_context_ptr);
        m_atomic_context_ptr = nullptr;

        m_meta_allocator.endAtomic();
        m_v_object_cache.endAtomic();        
        getGC0().endAtomic();
        Memspace::endAtomic();
    }
    
    void Fixture::cancelAtomic()
    {
        assert(m_atomic_context_ptr);
        m_atomic_context_ptr = nullptr;
        m_meta_allocator.cancelAtomic();
        getGC0().cancelAtomic();
        // rollback any uncommited changes
        rollback();
        // detach owned resources
        for (auto &detach: m_detach_handlers) {
            detach();
        }

        m_v_object_cache.detach();
        m_string_pool.detach();
        m_object_catalogue.detach();
        m_v_object_cache.cancelAtomic();
        Memspace::cancelAtomic();        
    }
    
    AtomicContext *Fixture::tryGetAtomicContext() const {
        return m_atomic_context_ptr;
    }

    void Fixture::forAllSlabs(std::function<void(const SlabAllocator &, std::uint32_t)> f) const {
        m_meta_allocator.forAllSlabs(f);
    }
    
    bool Fixture::onCacheFlushed(bool) const
    {
        // NOTE: prevent cleanups during commit to avoid unwanted side-effects
        if (m_commit_pending) {
            return false;
        }
        
        if (m_prefix) {
            m_prefix->cleanup();            
        }
        return true;
    }

    void Fixture::registerPrefixStateReachedCallback(StateNumType state_num, std::unique_ptr<StateReachedCallbackBase> &&callback)
    {
        std::unique_lock lock(m_commit_mutex);
        auto current_state_num = getPrefix().getStateNum(true);
        if(state_num <= current_state_num) {
            callback->execute();
            callback.reset();
            return;
        }

        auto [list_it, _] = m_state_num_callbacks.try_emplace(state_num);
        StateReachedCallbackList &list = list_it->second;
        list.push_back(std::move(callback));
    }

    Fixture::StateReachedCallbackList Fixture::collectStateReachedCallbacks()
    {
        auto prefix_ptr = getPrefixPtr();
        if (!prefix_ptr) {
            return {};
        }        

        StateReachedCallbackList result_callbacks;
        auto current_state_num = prefix_ptr->getStateNum(true);
        auto it = m_state_num_callbacks.begin();
        while (it != m_state_num_callbacks.end() && it->first <= current_state_num) {
            StateReachedCallbackList &state_num_callbacks = it->second;
            std::move(state_num_callbacks.begin(), state_num_callbacks.end(), std::back_inserter(result_callbacks));
            it = m_state_num_callbacks.erase(it);
        }
        return result_callbacks;
    }
    
    void Fixture::executeStateReachedCallbacks(const StateReachedCallbackList &callbacks)
    {
        // Notify state number observers
        for(const auto &callback : callbacks) {
            callback->execute();
        }
    }

    void Fixture::beginLocked(unsigned int locked_section_id)
    {
        m_mutation_log.beginLocked(locked_section_id);
        // also begin locked with registered handlers (e.g. TagIndex & Index)
        for (auto &handler: m_mutation_handlers) {
            handler->beginLocked(locked_section_id);            
        }
    }
    
    bool Fixture::endLocked(unsigned int locked_section_id)
    {        
        bool result = m_mutation_log.endLocked(locked_section_id);
        // also end locked with registered handlers (e.g. TagIndex)
        auto it = m_mutation_handlers.begin();
        while (it != m_mutation_handlers.end()) {
            result |= (*it)->endLocked(locked_section_id);
            // remove unreferenced handlers
            if ((*it).use_count() == 1) {
                m_mutation_log.add(**it);
                it = m_mutation_handlers.erase(it);
            } else {
                ++it;
            }
        }
        return result;
    }
    
    void Fixture::endAllLocked(std::function<void(unsigned int)> callback)
    {
        m_mutation_log.endAllLocked(callback);
        auto it = m_mutation_handlers.begin();
        while (it != m_mutation_handlers.end()) {
            (*it)->endAllLocked(callback);
            // remove unreferenced handlers
            if ((*it).use_count() == 1) {
                it = m_mutation_handlers.erase(it);
            } else {                
                ++it;
            }
        }
    }
    
    PrefixName Fixture::tryGetPrefixName() const
    {
        auto prefix_ptr = getPrefixPtr();
        return prefix_ptr ? PrefixName(prefix_ptr->getName()) : PrefixName();
    }
    
    bool is_same(const db0::weak_swine_ptr<Fixture> &fx_1, const db0::weak_swine_ptr<Fixture> &fx_2)
    {
        if (fx_1 == fx_2) {
            return true;
        }
        // compare actual Fixture instances by the underlying UUIDs
        return  *(fx_1.safe_lock()) == *(fx_2.safe_lock());
    }

    bool is_same(const db0::weak_swine_ptr<Fixture> &fx_1, const db0::swine_ptr<Fixture> &fx_2)
    {        
        if (fx_1 == fx_2) {
            return true;
        }
        // compare actual Fixture instances by the underlying UUIDs
        assert(fx_2);
        return  *(fx_1.safe_lock()) == *fx_2;
    }

    bool is_same(const db0::swine_ptr<Fixture> &fx_1, const db0::weak_swine_ptr<Fixture> &fx_2)
    {
        if (fx_2 == fx_1) {
            return true;
        }
        // compare actual Fixture instances by the underlying UUIDs
        assert(fx_1);
        return  *fx_1 == *(fx_2.safe_lock());
    }
    
}
