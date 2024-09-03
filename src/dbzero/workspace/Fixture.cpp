#include "Fixture.hpp"
#include <dbzero/core/memory/MetaAllocator.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/utils/uuid.hpp>
#include "GC0.hpp"
#include "Workspace.hpp"
#include "WorkspaceView.hpp"

namespace db0

{
        
    std::uint32_t slot_index(std::uint32_t slot_num) {
        return slot_num - 1;
    }

    o_fixture::o_fixture()
        : m_UUID(db0::make_UUID())
    {
    }
    
    Fixture::Fixture(Workspace &workspace, std::shared_ptr<Prefix> prefix, std::shared_ptr<MetaAllocator> meta)
        : Fixture(workspace, workspace.getSharedObjectList(), prefix, meta)
    {        
    }
    
    Fixture::Fixture(Snapshot &snapshot, FixedObjectList &shared_object_list, std::shared_ptr<Prefix> prefix, std::shared_ptr<MetaAllocator> meta)
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

    void Fixture::rollback()
    {
        for (auto &rollback: m_rollback_handlers) {
            rollback();
        }
    }
    
    void Fixture::close()
    {
        // clear cache to destroy object instances supported by the cache
        // this has to be done before commit (to not commit unrefereced objects)
        m_lang_cache.clear();
        // auto-commit before closing
        if (m_access_type == AccessType::READ_WRITE) {
            // perform the operation twice to ensure both pre-commit and commit
            onAutoCommit();
            onAutoCommit();
        }
        std::unique_lock<std::mutex> lock(m_close_mutex);
        for (auto &close: m_close_handlers) {
            close(false);
        }
        m_string_pool.close();
        Memspace::close();
    }
    
    bool Fixture::refresh()
    {
        assert(getAccessType() == AccessType::READ_ONLY && "Refresh only makes sense for read-only fixtures");
        m_updated = false;
        if (!Memspace::refresh()) {
            return false;
        }
        // detach all active v_object instances so that they can be refreshed        
        getGC0().detachAll();
        return true;
    }
    
    void Fixture::onUpdated()
    {    
        m_updated = true;
        // this is to prevent commits when the modifications are continued
        m_pre_commit = false;
    }
    
    void Fixture::refreshIfUpdated()
    {
        // only refresh read-only fixtures
        if (getAccessType() == AccessType::READ_ONLY && m_updated) {
            refresh();
        }
    }
    
    db0::swine_ptr<Fixture> Fixture::getSnapshot(Snapshot &workspace_view, std::optional<std::uint64_t> state_num) const
    {        
        auto px_snapshot = m_prefix->getSnapshot(state_num);
        auto allocator_snapshot = std::make_shared<MetaAllocator>(px_snapshot, m_meta_allocator.getSlabRecyclerPtr());
        return db0::make_swine<Fixture>(
            workspace_view, m_v_object_cache.getSharedObjectList(), px_snapshot, allocator_snapshot
        );
    }
    
    void Fixture::commit()
    {
        assert(getPrefixPtr());

        // pre-commit to prepare objects which require it (e.g. Index) for commit
        // NOTE: pre-commit must NOT lock the fixture's shared mutex
        if (m_gc0_ptr) {
            getGC0().preCommit();            
        }

        std::unique_lock<std::shared_mutex> lock(m_shared_mutex);        
        tryCommit(lock);
        m_pre_commit = false;
        m_updated = false;
    }
    
    void Fixture::tryCommit(std::unique_lock<std::shared_mutex> &lock)
    {
        auto prefix_ptr = getPrefixPtr();
        // prefix may not exist if fixture has already been closed
        if (!prefix_ptr) {
            return;
        }

        std::unique_ptr<GC0::CommitContext> gc0_ctx = m_gc0_ptr ? getGC0().beginCommit() : nullptr;
        // FIXME: this should be changed to commit-op
        if (m_gc0_ptr) {
            getGC0().detachAll();
        }
        
        for (auto &commit: m_close_handlers) {
            commit(true);
        }
        
        // commit garbage collector's state
        // we check if gc0 exists because the unit-tests set up may not have it
        if (gc0_ctx) {
            gc0_ctx->commit();
        }
        m_string_pool.commit();
        m_object_catalogue.commit();
        m_v_object_cache.commit();
        Memspace::commit();
    }
    
    void Fixture::onAutoCommit()
    {
        if (m_pre_commit) {
            // prevents commit on a closed fixture
            std::unique_lock<std::mutex> lock(m_close_mutex);
            if (!Memspace::isClosed()) {
                // pre-commit to prepare objects which require it (e.g. Index) for commit
                // NOTE: pre-commit must NOT lock the fixture's shared mutex
                if (m_gc0_ptr) {
                    getGC0().preCommit();            
                }

                // lock for exclusive access
                std::unique_lock<std::shared_mutex> lock(m_shared_mutex);            
                tryCommit(lock);
                m_pre_commit = false;
                m_updated = false;
            }
        }
        if (m_updated) {
            m_pre_commit = true;            
        }
    }
    
    db0::GC0 &Fixture::addGC0(db0::swine_ptr<Fixture> &fixture)
    {
        assert(!m_gc0_ptr);
        m_gc0_ptr = &addResource<db0::GC0>(fixture);
        return *m_gc0_ptr;
    }
    
    db0::GC0 &Fixture::addGC0(db0::swine_ptr<Fixture> &fixture, std::uint64_t address, bool read_only)
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

    std::uint64_t Fixture::makeRelative(std::uint64_t address, std::uint32_t slot_num) const {
        return reinterpret_cast<const SlabAllocator&>(m_slot_allocator.getSlot(slot_num)).makeRelative(address);
    }
    
    std::uint64_t Fixture::makeAbsolute(std::uint64_t address, std::uint32_t slot_num) const {
        return reinterpret_cast<const SlabAllocator&>(m_slot_allocator.getSlot(slot_num)).makeAbsolute(address);
    }
    
    bool Fixture::operator==(const Fixture &other) const {
        return m_UUID == other.m_UUID;
    }

    void Fixture::beginAtomic(AtomicContext *context)
    {
        assert(!m_atomic_context_ptr);        
        m_atomic_context_ptr = context;        
        getGC0().beginAtomic();
        
        for (auto &commit: m_close_handlers) {
            commit(true);
        }
        
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
        
        m_string_pool.detach();
        m_object_catalogue.detach();
    }
    
    void Fixture::endAtomic()
    {        
        assert(m_atomic_context_ptr);
        m_atomic_context_ptr = nullptr;

        m_v_object_cache.endAtomic();
        getGC0().endAtomic();
        Memspace::endAtomic();
    }
    
    void Fixture::cancelAtomic()
    {
        assert(m_atomic_context_ptr);
        m_atomic_context_ptr = nullptr;
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

}