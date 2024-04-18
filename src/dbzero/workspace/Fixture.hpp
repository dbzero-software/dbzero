#pragma once

#include <memory>
#include <deque>
#include <functional>
#include <shared_mutex>

#include <dbzero/core/memory/Memspace.hpp>
#include <dbzero/core/collections/pools/StringPools.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/vspace/db0_ptr.hpp>
#include "ResourceManager.hpp"
#include "DependencyWrapper.hpp"

#include <dbzero/object_model/LangCache.hpp>
#include <dbzero/core/memory/swine_ptr.hpp>
#include <dbzero/core/collections/full_text/FT_BaseIndex.hpp>
#include <dbzero/object_model/ObjectCatalogue.hpp>
#include <dbzero/core/memory/VObjectCache.hpp>

namespace db0

{
    
    class GC0;
    class MetaAllocator;
    class Workspace;
    using StringPoolT = db0::pools::RC_LimitedStringPool;
    using ObjectCatalogue = db0::object_model::ObjectCatalogue;

    /**
     * Fixture header placed at a fixed well-known address (e.g. 0x0)
    */
    struct [[gnu::packed]] o_fixture: public o_fixed<o_fixture>
    {
        // auto-generated fixture UUID
        std::uint64_t m_UUID;
        // address of the Object Catalogue
        std::uint64_t m_object_catalogue_address = 0;
        // address of the common limited string pool
        std::uint64_t m_limited_string_pool_address = 0;
        std::uint32_t m_limited_string_pool_size = 0;
        db0::db0_ptr<StringPoolT> m_string_pool_ptr;

        o_fixture();
    };
    
    struct FixtureLock;

    /**
     * Fixture is a Memspace extension with additionaly initialized common utilities:     
     * 1) Object catalogue
     * 2) Limited string pool (32 bit pointers)
     * 3) Tag inverted index
    */
    class Fixture: public Memspace
    {
    public:
        using LangCache = db0::object_model::LangCache;
        
        Fixture(Workspace &, std::shared_ptr<Prefix>, std::shared_ptr<MetaAllocator>);  
        Fixture(FixedObjectList &, std::shared_ptr<Prefix>, std::shared_ptr<MetaAllocator>);
        Fixture(Fixture const &) = delete;

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
        template <typename T> T &get() const
        {
            return m_resource_manager.select<T>();
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
        db0::GC0 &addGC0(db0::swine_ptr<Fixture> &fixture);
        db0::GC0 &addGC0(db0::swine_ptr<Fixture> &fixture, std::uint64_t address);
        
        // add commit or close handler (the actual operation identified by the boolean flag)
        void addCloseHandler(std::function<void(bool commit)>);

        void commit();
        
        void close();

        inline GC0 &getGC0() {
            assert(m_gc0_ptr);
            return *m_gc0_ptr;
        }

        inline const GC0 &getGC0() const {
            assert(m_gc0_ptr);
            return *m_gc0_ptr;
        }
        
        LangCache &getLangCache() const {
            return m_lang_cache;
        }
        
        VObjectCache &getVObjectCache() const {
            return m_v_object_cache;
        }
        
        /**
         * Retrieve the most recent updates made to this fixture by other processes
         * member only allowed for read-only fixtures
         * @return true if the fixture was updated
        */
        bool refresh();
        
        /**
         * This member checks m_updated flag before calling refresh
        */
        void refreshIfUpdated();
        
        /**
         * Get read-only snapshot of the fixture's current state
        */
        db0::swine_ptr<Fixture> getSnapshot(std::optional<std::uint64_t> state_num = {}) const;

        void onUpdated();
                     
    private:
        const std::uint64_t m_UUID;
        // the registry holds active v_ptr instances (important for refresh)
        // and cleanup of the "hanging" references
        db0::GC0 *m_gc0_ptr = nullptr;
        StringPoolT m_string_pool;
        ObjectCatalogue m_object_catalogue;
        // language-specific object cache
        mutable LangCache m_lang_cache;
        // internal cache for DBZero based collections
        mutable VObjectCache m_v_object_cache;

        // For read/write fixtures:
        // the onUpdate is called whenever the fixture is modified
        // For read-only fixtures:
        // the updates flag set to true means that the refresh thread detected external changes
        // and refresh might be possible
        std::atomic<bool> m_updated = false;
        // Pre-commit flag is set by the AutoCommitThread to mark the Fixture for commit
        std::atomic<bool> m_pre_commit = false;
        
        StringPoolT openLimitedStringPool(Memspace &, MetaAllocator &);

        db0::ObjectCatalogue openObjectCatalogue(MetaAllocator &);
        
        mutable ResourceManager m_resource_manager;
        std::deque<std::shared_ptr<db0::DependencyHolder> > m_dependencies;
        std::vector<std::function<void(bool)> > m_close_handlers;
        
        std::uint64_t getUUID(MetaAllocator &);

        // try commit if not closed yet
        void tryCommit();

    protected:
        friend class FixtureThread;
        friend class FixtureLock;
        friend class AutoCommitThread;
        std::shared_mutex m_shared_mutex;
        
        /**
         * Called by the AutoCommitThread
        */
        void onAutoCommit();
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
    template <typename T, typename... Args> T &Fixture::addResource(Args&&... args)
    {
        return addResourceAs<T, T>(std::forward<Args>(args)...);
    }
        
    struct FixtureLock
    {
        inline FixtureLock(const db0::swine_ptr<Fixture> &fixture)
            : m_fixture(fixture)
            , m_lock(fixture->m_shared_mutex)
        {
        }

        ~FixtureLock()
        {        
        }
        
        inline db0::swine_ptr<Fixture> &operator*() {
            return m_fixture;
        }
        
        inline Fixture *operator->() const {
            return m_fixture.get();
        }

        db0::swine_ptr<Fixture> m_fixture;
        std::shared_lock<std::shared_mutex> m_lock;
    };

}