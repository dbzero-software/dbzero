// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <memory>
#include <deque>
#include <functional>
#include <shared_mutex>
#include <map>

#include <dbzero/core/serialization/FixedVersioned.hpp>
#include <dbzero/core/memory/Memspace.hpp>
#include <dbzero/core/collections/pools/StringPools.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/vspace/db0_ptr.hpp>
#include <dbzero/core/compiler_attributes.hpp>
#include "ResourceManager.hpp"
#include "DependencyWrapper.hpp"
#include "MutationLog.hpp"
    
#include <dbzero/core/memory/swine_ptr.hpp>
#include <dbzero/core/collections/full_text/FT_BaseIndex.hpp>
#include <dbzero/object_model/ObjectCatalogue.hpp>
#include <dbzero/object_model/LangCache.hpp>
#include <dbzero/core/memory/VObjectCache.hpp>
#include <dbzero/core/memory/SlotAllocator.hpp>
#include <dbzero/core/utils/ProcessTimer.hpp>

namespace db0

{

DB0_PACKED_BEGIN
    
    class GC0;
    class MetaAllocator;
    class Snapshot;
    class Workspace;
    class WorkspaceView;
    class SlabAllocator;
    class AtomicContext;
    class LangCache;
    class ProcessTimer;
    using StringPoolT = db0::pools::RC_LimitedStringPool;
    using ObjectCatalogue = db0::object_model::ObjectCatalogue;

    struct DB0_PACKED_ATTR SlotDef
    {
        Address m_address = {};
        std::uint64_t m_size = 0;
    };
    
    /**
     * Fixture header placed at a fixed well-known address (e.g. 0x0)
    */
    struct DB0_PACKED_ATTR o_fixture: public o_fixed_versioned<o_fixture>
    {
        // auto-generated fixture UUID
        std::uint64_t m_UUID;
        // address of the Object Catalogue
        Address m_object_catalogue_address = {};
        // slot definitions
        SlotDef m_slots[8];
        db0::db0_ptr<StringPoolT> m_string_pool_ptr;

        o_fixture();
    };
    
    struct FixtureLock;

    class StateReachedCallbackBase
    {
    public:
        virtual ~StateReachedCallbackBase() = default;

        virtual void execute() = 0;
    };
    
    // Compare fixture pointers by the underlying UUIDs
    // i.e. yielding true even if taken from different snapshots
    bool is_same(const db0::weak_swine_ptr<Fixture> &, const db0::weak_swine_ptr<Fixture> &);
    bool is_same(const db0::weak_swine_ptr<Fixture> &, const db0::swine_ptr<Fixture> &);
    bool is_same(const db0::swine_ptr<Fixture> &, const db0::weak_swine_ptr<Fixture> &);
    
    /**
     * Fixture is a Memspace extension with additionaly initialized common utilities:     
     * 1) Object catalogue
     * 2) Limited string pool (32 bit pointers)
     * 3) Tag inverted index
    */
    class Fixture: public Memspace
    {
    public:
        using StateReachedCallbackList = std::vector<std::unique_ptr<StateReachedCallbackBase>>;

        // Limited String Pool's slot number
        static constexpr std::uint32_t LSP_SLOT_NUM = 1;
        // slot number for DB0 types and enums
        static constexpr std::uint32_t TYPE_SLOT_NUM = 2;
        
        // @param locked_sections - the number of active locked sections
        Fixture(Workspace &, std::shared_ptr<Prefix>, std::shared_ptr<MetaAllocator>, int locked_sections = 0);
        Fixture(Snapshot &, FixedObjectList &, std::shared_ptr<Prefix>, std::shared_ptr<MetaAllocator>,
            int locked_sections = 0);
        Fixture(Fixture const &) = delete;
        
        virtual ~Fixture();
        
        /**
         * Initialize a new fixture over existing memspace
         * must be called for each newly created memspace
         * @param memspace the memspace to initialize
         * @param meta the MetaAllocator instance associated with the memspace
        */
        static void formatFixture(Memspace, MetaAllocator &meta);

        StringPoolT &getLimitedStringPool() {
            return m_string_pool;
        }

        const StringPoolT &getLimitedStringPool() const {
            return m_string_pool;
        }

        db0::ObjectCatalogue &getObjectCatalogue() {
            return m_object_catalogue;
        }

        const db0::ObjectCatalogue &getObjectCatalogue() const {
            return m_object_catalogue;
        }
        
        std::uint64_t getUUID() const {
            return m_UUID;
        }
        
        static std::uint64_t getUUID(std::shared_ptr<Prefix>, MetaAllocator &);
        
        /**
         * Get resource from resource manager (convenience proxy)
         * @tparam T
         * @return instance of T
         */
        template <typename T> T &get() const {
            return m_resource_manager.select<T>();
        }

        template <typename T> T *tryGet() const {
            return m_resource_manager.trySelect<T>();
        }

        template <typename T, typename ResultT, typename... Args> ResultT &addResourceAs(Args&&... args);

        /**
         * Create dependent resource using specific arguments (args) and register it with ResourceManager
         * @tparam T resource type (exported)
         * @tparam args
         */
        template <typename T, typename... Args> T &addResource(Args&&... args);

        /**
         * Create GC0 as a resource
        */
        db0::GC0 &createGC0(db0::swine_ptr<Fixture> &fixture);
        db0::GC0 &createGC0(db0::swine_ptr<Fixture> &fixture, Address, bool read_only);
        
        // add commit or close handler (the actual operation identified by the boolean flag)
        void addCloseHandler(std::function<void(bool commit)>);
        void addDetachHandler(std::function<void()>);
        void addRollbackHandler(std::function<void()>);
        void addFlushHandler(std::function<void()>);

        // @return the mutation log to be held / updated by the client object (e.g. Index)
        std::shared_ptr<MutationLog> addMutationHandler();
        
        // Rollback uncommited contents from internal buffers
        void rollback();
        
        // @return true if the fixture state was changed (i.e. actual transaction was committed)
        bool commit();
        
        void close(bool as_defunct, ProcessTimer * = nullptr);
        
        // Flush internal buffers of the associated resources (to free up memory)
        void flush();

        inline GC0 *tryGetGC0() const {
            return m_gc0_ptr;
        }

        inline GC0 &getGC0()
        {
            assert(m_gc0_ptr);
            return *m_gc0_ptr;
        }

        inline const GC0 &getGC0() const
        {
            assert(m_gc0_ptr);
            return *m_gc0_ptr;
        }

        VObjectCache &getVObjectCache() const {
            return m_v_object_cache;
        }
        
        /**
         * Retrieve the most recent updates made to this fixture by other processes
         * member only allowed for read-only fixtures
         * @return true if the fixture was updated
        */
        bool refresh(ProcessTimer * = nullptr);
        
        /**
         * This member checks m_updated flag before calling refresh
        */
        bool refreshIfUpdated();
        
        /**
         * Get read-only snapshot of the fixture's state within a specific WorkspaceView
        */
        db0::swine_ptr<Fixture> getSnapshot(Snapshot &, std::optional<std::uint64_t> state_num) const;

        /**
         * Mark this fixture for commit
        */
        void onUpdated();
        
        StateReachedCallbackList onRefresh();

        /**
         * Get the Snapshot interface of the related workspace
        */
        const Snapshot &getWorkspace() const;

        Snapshot &getWorkspace();

        // Converts address from a specific slot to relative one (i.e. within-slot offset)
        std::uint64_t makeRelative(Address address, std::uint32_t slot_num) const;
        // Converts a relative address back to absolute one
        Address makeAbsolute(std::uint64_t offset, std::uint32_t slot_num) const;

        inline AccessType getAccessType() const {
            return m_access_type;
        }
        
        bool operator==(const Fixture &) const;
        bool operator!=(const Fixture &) const;
        
        void preAtomic();
        
        void beginAtomic(AtomicContext *context);
        
        void endAtomic();
        
        void cancelAtomic();
        
        void detach();

        AtomicContext *tryGetAtomicContext() const;
        
        // Visit all slabs from the underlying meta-allocator
        void forAllSlabs(std::function<void(const SlabAllocator &, std::uint32_t slab_id)>) const;
        
        inline LangCacheView &getLangCache() const {
            return m_lang_cache;
        }
        
        const MetaAllocator &getMetaAllocator() const {
            return m_meta_allocator;
        }
        
        // Called by the CacheRecycler when cache limit has been reached
        // @return false if unable to handle this event at this time
        bool onCacheFlushed(bool threshold_reached) const;

        // Registers a new callback to be called after given prefix state number is reached
        // Fixture takes ownership of the callback
        void registerPrefixStateReachedCallback(StateNumType state_num, std::unique_ptr<StateReachedCallbackBase> &&callback);
        
        PrefixName tryGetPrefixName() const;
        
    private:
        const AccessType m_access_type;
        Snapshot &m_snapshot;
        // LangCache from the related workspace
        mutable LangCacheView m_lang_cache;
        // Underlying allocator's convenience references
        SlotAllocator &m_slot_allocator;
        MetaAllocator &m_meta_allocator;
        const std::uint64_t m_UUID;
        // the registry holds active v_object instances (important for refresh)
        // and cleanup of the "hanging" references
        db0::GC0 *m_gc0_ptr = nullptr;
        StringPoolT m_string_pool;
        ObjectCatalogue m_object_catalogue;
        // internal cache for dbzero based collections
        mutable VObjectCache m_v_object_cache;
        AtomicContext *m_atomic_context_ptr = nullptr;
        std::atomic<bool> m_closed = false;
        std::atomic<bool> m_commit_pending = false;

        // For read/write fixtures:
        // the onUpdated is called whenever the fixture is modified
        std::atomic<bool> m_updated = false;
                
        StringPoolT openLimitedStringPool(Memspace &, MetaAllocator &);
        
        std::shared_ptr<SlabAllocator> openSlot(Memspace &, MetaAllocator &, std::uint32_t slot_id);

        db0::ObjectCatalogue openObjectCatalogue(MetaAllocator &);
        
        mutable ResourceManager m_resource_manager;
        std::deque<std::shared_ptr<db0::DependencyHolder> > m_dependencies;
        std::vector<std::function<void(bool)> > m_close_handlers;
        std::vector<std::function<void()> > m_detach_handlers;
        std::vector<std::function<void()> > m_rollback_handlers;
        // flush handlers, to release some memory on resource exhaustion
        std::vector<std::function<void()> > m_flush_handlers;
        std::list<std::shared_ptr<MutationLog> > m_mutation_handlers;
        
        std::uint64_t getUUID(MetaAllocator &);
        
        // try commit if not closed yet
        // @return true if the underlying transaction's state number was changed
        bool tryCommit(std::unique_lock<std::shared_mutex> &, ProcessTimer * = nullptr);

        static std::shared_ptr<SlabAllocator> openSlot(MetaAllocator &, const v_object<o_fixture> &, std::uint32_t slot_id);
        
    protected:
        friend class FixtureThread;
        friend class FixtureThreadCallbacksContext;
        friend struct FixtureLock;
        friend class AutoCommitThread;
        friend class Workspace;
        mutable std::shared_mutex m_commit_mutex;
        mutable std::mutex m_close_mutex;
        mutable MutationLog m_mutation_log;

        void beginLocked(unsigned int locked_section_id);
        bool endLocked(unsigned int locked_section_id);
        // ends all locked sections, invokes callback for all mutated ones
        void endAllLocked(std::function<void(unsigned int)> callback);
        
        std::map<StateNumType, StateReachedCallbackList> m_state_num_callbacks;

        /**
         * Obtain the list of callbacks to be called for current prefix state number
         * Returned callbacks are removed from the Fixture's internal list
         * Note: This is a part of the mechanism to ensure thread-safety
         */
        StateReachedCallbackList collectStateReachedCallbacks();

        /**
         * Execute a given list of callbacks
         * Note: The code in these callbacks can be called from a different thread and may need to access the fixture or other parts of the API,
         * so it's important to ensure that held locks are released before calling this function! Otherwise a deadlock can occur.
         */
        void executeStateReachedCallbacks(const StateReachedCallbackList &callbacks);
        
        /**
         * Called by the AutoCommitThread
         * @return the list of callbacks to be executed when committing process was completed
        */
        StateReachedCallbackList onAutoCommit();
    };
    
    template <typename T, typename ResultT, typename... Args> ResultT &Fixture::addResourceAs(Args&&... args)
    {
        auto ptr = std::make_shared<T>(std::forward<Args>(args)...);
        auto dh = std::make_shared<db0::DependencyWrapper<T> >(ptr);
        this->m_dependencies.emplace_back(dh);
        auto &res = reinterpret_cast<ResultT&>(*ptr);
        m_resource_manager.addResource(res);
        return res;
    }
    
    /**
     * Create dependent resource using specific arguments (args) and register it with ResourceManager
     * @tparam T resource type (exported)
     * @tparam args
     */
    template <typename T, typename... Args> T &Fixture::addResource(Args&&... args) {
        return addResourceAs<T, T>(std::forward<Args>(args)...);
    }
    
    struct FixtureLock
    {
        inline FixtureLock(const db0::swine_ptr<Fixture> &fixture)
            : m_fixture(fixture)
            , m_lock(fixture->m_commit_mutex)
        {
            if (fixture->getAccessType() != AccessType::READ_WRITE) {
                THROWF(db0::InputException) << "Cannot modify read-only prefix: " << fixture->getPrefix().getName();
            }
            m_fixture->onUpdated();
        }
        
        ~FixtureLock()
        {        
        }
        
        inline db0::swine_ptr<Fixture> &operator*() 
        {
            m_fixture->onUpdated();
            return m_fixture;
        }
        
        inline Fixture *operator->() const
        {
            m_fixture->onUpdated();
            return m_fixture.get();
        }

        db0::swine_ptr<Fixture> m_fixture;
        std::shared_lock<std::shared_mutex> m_lock;
    };
    
DB0_PACKED_END

}