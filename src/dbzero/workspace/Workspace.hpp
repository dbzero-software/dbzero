#pragma once

#include <memory>
#include <unordered_map>
#include <functional>
#include <dbzero/core/memory/Memspace.hpp>
#include <dbzero/core/memory/CacheRecycler.hpp>
#include <dbzero/core/memory/swine_ptr.hpp>
#include <dbzero/core/memory/SlabRecycler.hpp>
#include <dbzero/core/memory/PrefixImpl.hpp>
#include <dbzero/core/memory/VObjectCache.hpp>
#include <dbzero/core/storage/Storage0.hpp>
#include <dbzero/core/storage/BDevStorage.hpp>
#include "Fixture.hpp"
#include <thread>
#include <filesystem>
#include "PrefixCatalog.hpp"
#include "Snapshot.hpp"
    
namespace db0

{

    class RefreshThread;
    class AutoCommitThread;
    class AtomicContext;

    class BaseWorkspace
    {
    public:
        // 4KB pages
        static constexpr std::size_t DEFAULT_PAGE_SIZE = 4096;
        // 16KB sparse index index (memory pages)
        static constexpr std::size_t DEFAULT_SPARSE_INDEX_NODE_SIZE = 16 * 1024 - 256;
        // 64MB slabs
        static constexpr std::size_t DEFAULT_SLAB_SIZE = 64 * 1024 * 1024;
        static constexpr std::size_t DEFAULT_CACHE_SIZE = 2u << 30;
        static constexpr std::size_t DEFAULT_SLAB_CACHE_SIZE = 256;
        
        // use block device storage
        using StorageT = BDevStorage;
        using PrefixT = PrefixImpl<StorageT>;
        
        /**
         * @param root_path default search path for existing prefixes and storage for new ones (pass "" for current directory)
         **/        
        BaseWorkspace(const std::string &root_path = "", std::optional<std::size_t> cache_size = {},
            std::optional<std::size_t> slab_cache_size = {});
        virtual ~BaseWorkspace()= default;
        
        /**
         * Create new or open existing prefix associated memspace
         * either for read-only or read-write access. Default is read-write
         * if state number if specified, the memspace will be opened in read-only mode to access a specific historical state (time-travel)
         * @param page_size required only when creating a new prefix
        */
        Memspace &getMemspace(const std::string &prefix_name, AccessType = AccessType::READ_WRITE, std::optional<std::size_t> page_size = {}, 
            std::optional<std::size_t> slab_size = {}, std::optional<std::size_t> sparse_index_node_size = {});
        
        bool hasMemspace(const std::string &prefix_name) const;
        
        /**
         * Commit all underlying read/write prefixes
        */
        void commit();
        
        /**
         * Flush any changes done to and close the memspace
        */
        bool close(const std::string &prefix_name);
        
        /**
         * Close & drop specific prefix
         * @return true if prefix was dropped
        */
        bool drop(const std::string &prefix_name, bool if_exists = true);

        /**
         * Close all prefixes, drop all uncommited data
        */
        void close();

        CacheRecycler &getCacheRecycler() {
            return m_cache_recycler;
        }

        const CacheRecycler &getCacheRecycler() const {
            return m_cache_recycler;
        }

        const PrefixCatalog &getPrefixCatalog() const {
            return m_prefix_catalog;
        }
        
    protected:
        PrefixCatalog m_prefix_catalog;
        
        /**
         * Open existing or create new memspace
        */
        std::pair<std::shared_ptr<Prefix>, std::shared_ptr<MetaAllocator> > openMemspace(const std::string &prefix_name, 
            bool &new_file_created, AccessType = AccessType::READ_WRITE, std::optional<std::size_t> page_size = {}, 
            std::optional<std::size_t> slab_size = {},
            std::optional<std::size_t> sparse_index_node_size = {});
        
    private:        
        CacheRecycler m_cache_recycler;
        SlabRecycler m_slab_recycler;
        // memspace by name
        std::unordered_map<std::string, Memspace> m_memspaces;
    };

    /**
     * Workspace extends BaseWorkspace and provides access to Fixtures instead of the raw Memspaces
    */
    class Workspace: protected BaseWorkspace, public Snapshot
    {
    public:
        static constexpr std::uint32_t DEFAULT_AUTOCOMMIT_INTERVAL_MS = 250;
        static constexpr std::size_t DEFAULT_VOBJECT_CACHE_SIZE = 16384;

        Workspace(const std::string &root_path = "", std::optional<std::size_t> cache_size = {},
            std::optional<std::size_t> slab_cache_size = {}, std::optional<std::size_t> vobject_cache_size = {},
            std::function<void(db0::swine_ptr<Fixture> &, bool is_new)> fixture_initializer = {});            
        virtual ~Workspace();
        
        // Set or change the autocommit interval in milliseconds
        void setAutocommitInterval(std::uint64_t interval_ms);

        bool hasFixture(const std::string &prefix_name) const override;

        /**
         * Get current fixture for either read-only or read-write access
        */
        db0::swine_ptr<Fixture> getCurrentFixture() override;
        
        /**
         * Create new or open/get existing prefix associated fixture
         * access type must be provided when the prefix is accessed for the 1st time
         */
        swine_ptr<Fixture> getFixtureEx(const std::string &prefix_name, std::optional<AccessType> = AccessType::READ_WRITE,
            std::optional<std::size_t> page_size = {}, std::optional<std::size_t> slab_size = {}, 
            std::optional<std::size_t> sparse_index_node_size = {},
            bool autocommit = true);
        
        /**
         * Get existing fixture by UUID
         * if access type is specified then auto-open is also attmpted
        */
        db0::swine_ptr<Fixture> getFixture(std::uint64_t uuid, std::optional<AccessType> = {}) override;
        
        db0::swine_ptr<Fixture> getFixture(const std::string &prefix_name, 
            std::optional<AccessType> = AccessType::READ_WRITE) override;
    
        /**
         * Find existing (opened) fixture or return nullptr
        */
        db0::swine_ptr<Fixture> tryFindFixture(const std::string &prefix_name) const;
        
        /**
         * Find existing (opened) fixture or throw
        */
        db0::swine_ptr<Fixture> findFixture(const std::string &prefix_name) const;

        /**
         * Commit all underlying read/write prefixes
        */
        void commit();
        
        /**
         * Commit specific prefix (must be opened as read/write)
        */
        void commit(const std::string &prefix_name);

        /**
         * Open specific prefix and make it the default one
         * @param prefix_name
         * @param access_type
         * @param autocommit flag indicating if the prefix should be auto-committed
        */
        void open(const std::string &prefix_name, AccessType access_type, bool autocommit = true,
            std::optional<std::size_t> slab_size = {});
        
        bool drop(const std::string &prefix_name, bool if_exists = true);

        bool close(const std::string &prefix_name) override;

        /**
         * Close all prefixes, drop all uncommited data
        */
        void close() override;
        
        CacheRecycler &getCacheRecycler();

        const CacheRecycler &getCacheRecycler() const;
        
        /**
         * Try refreshing all underlying fixtures
         * @return true if any fixture was refreshed
        */
        bool refresh();
        
        void forAll(std::function<void(const Fixture &)> callback) const;
        
        std::function<void(db0::swine_ptr<Fixture> &, bool is_new)> getFixtureInitializer() const;

        FixedObjectList &getSharedObjectList() const;

        // Get current fixture UUID
        std::optional<std::uint64_t> getDefaultUUID() const;
        
        void beginAtomic(AtomicContext *context);
        void detach();
        void endAtomic();
        
        void cancelAtomic();

    private:
        FixtureCatalog m_fixture_catalog;
        std::function<void(db0::swine_ptr<Fixture> &, bool is_new)> m_fixture_initializer;
        // fixture by UUID
        std::unordered_map<std::uint64_t, db0::swine_ptr<Fixture> > m_fixtures;
        std::vector<std::thread> m_threads;
        std::unique_ptr<RefreshThread> m_refresh_thread;
        std::unique_ptr<AutoCommitThread> m_auto_commit_thread;
        swine_ptr<db0::Fixture> m_default_fixture;
        std::vector<std::string> m_current_prefix_history;
        // shared object list is for maintainig v_object cache evition policy at a process level
        mutable FixedObjectList m_shared_object_list;
        // flag indicating atomic operation in progress
        AtomicContext *m_atomic_context_ptr = nullptr;

        std::optional<std::uint64_t> getUUID(const std::string &prefix_name) const;
    };
    
}