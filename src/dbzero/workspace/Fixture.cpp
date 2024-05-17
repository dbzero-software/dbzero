#include "Fixture.hpp"
#include <dbzero/core/memory/MetaAllocator.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/utils/uuid.hpp>
#include "GC0.hpp"
#include "Workspace.hpp"
#include "WorkspaceView.hpp"

namespace db0

{

    o_fixture::o_fixture()
        : m_UUID(db0::make_UUID())
    {
    }
    
    Fixture::Fixture(Workspace &workspace, std::shared_ptr<Prefix> prefix, std::shared_ptr<MetaAllocator> meta)
        : Fixture(workspace, workspace.getSharedObjectList(), prefix, meta)
    {        
    }
    
    Fixture::Fixture(Snapshot &snapshot, FixedObjectList &shared_object_list, std::shared_ptr<Prefix> prefix, std::shared_ptr<MetaAllocator> meta)
        : Memspace(prefix, meta, getUUID(prefix, *meta))
        , m_snapshot(snapshot)
        , m_UUID(*m_derived_UUID)
        , m_string_pool(openLimitedStringPool(*this, *meta))
        , m_slot_1(openSlot(*this, *meta, 1))
        , m_object_catalogue(openObjectCatalogue(*meta))
        , m_v_object_cache(*this, shared_object_list)
    {
    }
    
    StringPoolT Fixture::openLimitedStringPool(Memspace &memspace, MetaAllocator &meta)
    {
        using v_fixture = v_object<o_fixture>;
        
        // read fixture configuration from under the 1st address
        v_fixture fixture(this->myPtr(meta.getFirstAddress()));
        // open slot-0 for the exclusive use of the limited string pool
        auto lsp_slot = openSlot(meta, fixture, 0);
        return StringPoolT(Memspace(this->getPrefixPtr(), lsp_slot), memspace.myPtr(fixture->m_string_pool_ptr.getAddress()));
    }

    std::shared_ptr<SlabAllocator> Fixture::openSlot(Memspace &memspace, MetaAllocator &meta, std::uint32_t slot_id) 
    {
        using v_fixture = v_object<o_fixture>;        
        v_fixture fixture(this->myPtr(meta.getFirstAddress()));
        return openSlot(meta, fixture, slot_id);
    }

    std::shared_ptr<SlabAllocator> Fixture::openSlot(MetaAllocator &meta, const v_object<o_fixture> &fixture, std::uint32_t slot_id) {
        return meta.openReservedSlab(fixture->m_slots[slot_id].m_address, fixture->m_slots[slot_id].m_size);
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
            auto slot_0 = meta.reserveNewSlab();
            fixture.modify().m_slots[0] = { slot_0->getAddress(), slot_0->getSlabSize() };
            // create the string pool object
            StringPoolT string_pool(Memspace(memspace.getPrefixPtr(), slot_0), memspace);
            fixture.modify().m_string_pool_ptr = string_pool;
        }

        // create SLOT-1 (with the purpose to store Class and Enum objects)
        {
            auto slot_1 = meta.reserveNewSlab();
            fixture.modify().m_slots[1] = { slot_1->getAddress(), slot_1->getSlabSize() };
        }

        // create the Object Catalogue
        ObjectCatalogue object_catalogue(memspace);
        fixture.modify().m_object_catalogue_address = object_catalogue.getAddress();
    }

    void Fixture::addCloseHandler(std::function<void(bool)> f) {
        m_close_handlers.push_back(f);
    }
    
    void Fixture::close()
    {
        for (auto &f: m_close_handlers) {
            f(false);
        }
        m_string_pool.close();
        m_slot_1 = nullptr;
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
    
    db0::swine_ptr<Fixture> Fixture::getSnapshot(WorkspaceView &workspace_view) const
    {
        auto state_num = workspace_view.getStateNum();
        auto prefix_snapshot = m_prefix->getSnapshot(state_num);
        auto allocator_snapshot = std::make_shared<MetaAllocator>(
            prefix_snapshot, std::dynamic_pointer_cast<MetaAllocator>(m_allocator)->getSlabRecyclerPtr());
                
        return db0::make_swine<Fixture>(
            workspace_view, m_v_object_cache.getSharedObjectList(), prefix_snapshot, allocator_snapshot
        );
    }
    
    void Fixture::commit()
    {
        assert(getPrefixPtr());
        tryCommit();
    }

    void Fixture::tryCommit()
    {
        auto prefix_ptr = getPrefixPtr();
        // prefix may not exist if fixture has already been closed
        if (!prefix_ptr) {
            return;
        }
        
        for (auto &f: m_close_handlers) {
            f(true);
        }

        // commit garbage collector's state
        // we check if gc0 exists because the unit-tests set up may not have it
        if (m_gc0_ptr) {
            m_gc0_ptr->commit();
        }
        m_string_pool.commit();
        m_object_catalogue.commit();
        Memspace::commit();
    }
    
    void Fixture::onAutoCommit()
    {
        if (m_pre_commit) {
            // lock for exclusive access
            std::unique_lock<std::shared_mutex> lock(m_shared_mutex);
            tryCommit();
            m_pre_commit = false;
            m_updated = false;
        }
        if (m_updated) {
            // assign form commit
            m_pre_commit = true;            
        }
    }
    
    db0::GC0 &Fixture::addGC0(db0::swine_ptr<Fixture> &fixture)
    {
        assert(!m_gc0_ptr);
        m_gc0_ptr = &addResource<db0::GC0>(fixture, m_lang_cache);
        return *m_gc0_ptr;
    }

    db0::GC0 &Fixture::addGC0(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
    {
        assert(!m_gc0_ptr);
        m_gc0_ptr = &addResource<db0::GC0>(fixture, address, m_lang_cache);
        return *m_gc0_ptr;
    }

    const Snapshot &Fixture::getWorkspace() const {
        return m_snapshot;
    }
    
    Snapshot &Fixture::getWorkspace() {
        return m_snapshot;
    }
    
}