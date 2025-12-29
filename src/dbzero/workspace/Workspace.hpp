// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <memory>
#include <unordered_map>
#include <list>
#include <functional>
#include <atomic>
#include <vector>
#include <unordered_set>
#include <dbzero/core/memory/Memspace.hpp>
#include <dbzero/core/memory/CacheRecycler.hpp>
#include <dbzero/core/memory/swine_ptr.hpp>
#include <dbzero/core/memory/SlabItem.hpp>
#include <dbzero/core/memory/Recycler.hpp>
#include <dbzero/core/memory/SlabAllocatorConfig.hpp>
#include <dbzero/core/memory/PrefixImpl.hpp>
#include <dbzero/core/memory/VObjectCache.hpp>
#include <dbzero/core/storage/Storage0.hpp>
#include <dbzero/core/storage/BDevStorage.hpp>
#include <dbzero/core/utils/weak_vector.hpp>
#include "Fixture.hpp"
#include <filesystem>
#include "PrefixCatalog.hpp"
#include "Snapshot.hpp"
#include "LockFlags.hpp"
    
namespace db0

{

    class RefreshThread;
    class AutoCommitThread;
    class AtomicContext;
    class LangCache;
    class Config;
    class WorkspaceView;
    using SlabRecycler = Recycler<SlabItem>;
    
    class BaseWorkspace
    {
    public:
        static constexpr std::size_t DEFAULT_PAGE_SIZE = SlabAllocatorConfig::DEFAULT_PAGE_SIZE;
        static constexpr std::size_t DEFAULT_SLAB_SIZE = SlabAllocatorConfig::DEFAULT_SLAB_SIZE;

        // 16KB sparse index index (memory pages)
        static constexpr std::size_t DEFAULT_SPARSE_INDEX_NODE_SIZE = 16 * 1024 - 256;
        static constexpr std::size_t DEFAULT_CACHE_SIZE = 2u << 30;
        static constexpr std::size_t DEFAULT_SLAB_CACHE_SIZE = 256;
        
        /**
         * @param root_path default search path for existing prefixes and storage for new ones (pass "" for current directory)
         **/        
        BaseWorkspace(const std::string &root_path = "", std::optional<std::size_t> cache_size = {},
            std::optional<std::size_t> slab_cache_size = {}, std::optional<std::size_t> flush_size = {},
            std::optional<LockFlags> default_lock_flags = {});
        virtual ~BaseWorkspace()= default;
        
        /**
         * Create new or open existing prefix associated memspace
         * either for read-only or read-write access. Default is read-write
         * if state number if specified, the memspace will be opened in read-only mode to access a specific historical state (time-travel)
         * @param page_size required only when creating a new prefix
        */
        Memspace &getMemspace(const PrefixName &, AccessType = AccessType::READ_WRITE, std::optional<std::size_t> page_size = {},
            std::optional<std::size_t> slab_size = {}, std::optional<std::size_t> sparse_index_node_size = {});
        
        bool hasMemspace(const PrefixName &) const;
        
        /**
         * Commit all underlying read/write prefixes
         * @return true if state was changed in at least one prefix
        */
        bool commit();
        
        /**
         * Flush any changes done to and close the memspace
        */
        bool close(const PrefixName &);
        
        /**
         * Close & drop specific prefix
         * @return true if prefix was dropped
        */
        bool drop(const PrefixName &, bool if_exists = true);

        /**
         * Close all prefixes, commit all data from read/write prefixes
        */
        void close(ProcessTimer * = nullptr);
        
        CacheRecycler &getCacheRecycler() {
            return m_cache_recycler;
        }

        const CacheRecycler &getCacheRecycler() const {
            return m_cache_recycler;
        }

        const PrefixCatalog &getPrefixCatalog() const {
            return m_prefix_catalog;
        }
        
        void setCacheSize(std::size_t cache_size);

    protected:
        PrefixCatalog m_prefix_catalog;
        // the variable to hold total size of all "dirty" locks in the entire workspace
        std::atomic<std::size_t> m_dirty_meter = 0;
        LockFlags m_default_lock_flags;

        /**
         * Open existing or create new memspace
        */
        std::pair<std::shared_ptr<Prefix>, std::shared_ptr<MetaAllocator> > openMemspace(const PrefixName &,
            bool &new_file_created, AccessType = AccessType::READ_WRITE, std::optional<std::size_t> page_size = {}, 
            std::optional<std::size_t> slab_size = {}, std::optional<std::size_t> sparse_index_node_size = {},
            std::optional<LockFlags> lock_flags = {}, std::optional<std::size_t> meta_io_step_size = {},
            std::optional<std::size_t> page_io_step_size = {}
        );
        
        // Clear all internal in-memory caches
        void clearCache() const;

        virtual bool onCacheFlushed(bool threshold_reached) const;

        virtual void forEachMemspace(std::function<bool(Memspace &)> callback);

        // try releasing a specific volume of dirty locks
        virtual void onFlushDirty(std::size_t limit);

    private:
        mutable CacheRecycler m_cache_recycler;
        SlabRecycler m_slab_recycler;
        // memspace by name
        std::unordered_map<std::string, Memspace> m_memspaces;
    };

    class WorkspaceThreads;

    /**
     * Workspace extends BaseWorkspace and provides access to Fixtures instead of the raw Memspaces
    */
    class Workspace: protected BaseWorkspace, public Snapshot
    {
    public:        
        // NOTE: the default autocommit intarval (367ms) allows 50 years of continuous operation with 32-bit state numbers
        static constexpr std::uint32_t DEFAULT_AUTOCOMMIT_INTERVAL_MS = 367;
        static constexpr std::size_t DEFAULT_VOBJECT_CACHE_SIZE = 16384;
        
        Workspace(const std::string &root_path = "", std::optional<std::size_t> cache_size = {},
            std::optional<std::size_t> slab_cache_size = {}, std::optional<std::size_t> flush_size = {},
            std::optional<std::size_t> vobject_cache_size = {},
            std::function<void(db0::swine_ptr<Fixture> &, bool, bool, bool)> fixture_initializer = {},
            std::shared_ptr<Config> config = nullptr,
            std::optional<LockFlags> default_lock_flags = {});
        virtual ~Workspace();
        
        // Set or change the autocommit interval in milliseconds
        void setAutocommitInterval(std::uint64_t interval_ms);

        bool hasFixture(const PrefixName &prefix_name) const override;
        
        /**
         * Get current fixture for either read-only or read-write access
        */
        db0::swine_ptr<Fixture> getCurrentFixture() override;
        
        /**
         * Create new or open/get existing prefix associated fixture
         * access type must be provided when the prefix is accessed for the 1st time
         */
        swine_ptr<Fixture> tryGetFixtureEx(const PrefixName &, std::optional<AccessType> = AccessType::READ_WRITE,
            std::optional<std::size_t> page_size = {}, std::optional<std::size_t> slab_size = {}, 
            std::optional<std::size_t> sparse_index_node_size = {},
            std::optional<bool> autocommit = {}, std::optional<LockFlags> lock_flags = {},
            std::optional<std::size_t> meta_io_step_size = {}, 
            std::optional<std::size_t> page_io_step_size = {});
        
        swine_ptr<Fixture> getFixtureEx(const PrefixName &, std::optional<AccessType> = AccessType::READ_WRITE,
            std::optional<std::size_t> page_size = {}, std::optional<std::size_t> slab_size = {}, 
            std::optional<std::size_t> sparse_index_node_size = {},
            std::optional<bool> autocommit = {}, std::optional<LockFlags> lock_flags = {},
            std::optional<std::size_t> meta_io_step_size = {}, 
            std::optional<std::size_t> page_io_step_size = {});
        
        /**
         * Get existing fixture by UUID
         * if access type is specified then auto-open is also attmpted
        */
        db0::swine_ptr<Fixture> tryGetFixture(std::uint64_t uuid, std::optional<AccessType> = {}) override;        
        
        db0::swine_ptr<Fixture> tryGetFixture(const PrefixName &prefix_name,
            std::optional<AccessType> = {}) override;
        
        /**
         * Find existing (opened) fixture or return nullptr
        */
        db0::swine_ptr<Fixture> tryFindFixture(const PrefixName &) const override;
        
        std::size_t size() const override;
        
        /**
         * Commit all underlying read/write prefixes
         * @return true if state was changed in at least one prefix
        */
        bool commit();
        
        /**
         * Commit specific prefix (must be opened as read/write)
         * @return true if state was changed in the prefix
        */
        bool commit(const PrefixName &);
        
        // Flush from any internal write buffers (e.g. tags / index updates)
        void flush();
        
        /**
         * Open specific prefix and make it the default one
         * @param prefix_name
         * @param access_type
         * @param autocommit flag indicating if the prefix should be auto-committed
         * @param meta_io_step_size the size of the step in the underlying MetaIOStream (16MB by default)
         * @param page_io_step_size parameter only respected for newly created prefixes
        */
        void open(const PrefixName &, AccessType access_type, std::optional<bool> autocommit = {},
            std::optional<std::size_t> slab_size = {}, std::optional<LockFlags> default_lock_flags = {}, 
            std::optional<std::size_t> meta_io_step_size = {}, std::optional<std::size_t> page_io_step_size = {}
        );
        
        bool drop(const PrefixName &, bool if_exists = true);

        bool close(const PrefixName &) override;
        
        /**
         * Close all prefixes, commit all data from read/write prefixes
         * @param as_defunct if true (i.e. Python interpreter is already shut down) then close without releasing any
         * language specific resources
        */
        void close(bool as_defunct = false, ProcessTimer * = nullptr) override;
        
        std::shared_ptr<LangCache> getLangCache() const override;

        bool isMutable() const override;
        
        CacheRecycler &getCacheRecycler();

        const CacheRecycler &getCacheRecycler() const;
        
        /**
         * Try refreshing all underlying fixtures
         * @return true if any fixture was refreshed
        */
        bool refresh(bool if_updated = false);
        
        void forEachFixture(std::function<bool(const Fixture &)>) const;
        
        // Register a callback function to be invoked each time when a fixture is opened or created
        // this is used to register known Class and language specific type bindings within the fixture
        void setOnOpenCallback(std::function<void(db0::swine_ptr<Fixture> &, bool is_new)> callback);
        
        std::function<void(db0::swine_ptr<Fixture> &, bool is_new, bool is_read_only, bool is_snapshot)>
        getFixtureInitializer() const;

        FixedObjectList &getSharedObjectList() const;

        // Get current fixture UUID
        std::optional<std::uint64_t> getDefaultUUID() const;
        
        // prepare for an atomic operation - e.g. by flushing internal update buffers
        void preAtomic();
        void beginAtomic(AtomicContext *context);
        void detach();
        void endAtomic();
        
        void cancelAtomic();
        
        void setCacheSize(std::size_t cache_size);

        // Clear all internal in-memory caches
        void clearCache() const;
        
        const FixtureCatalog &getFixtureCatalog() const;
        
        std::shared_ptr<WorkspaceView> getWorkspaceView(
            std::optional<std::uint64_t> state_num = {},
            const std::unordered_map<std::string, std::uint64_t> &prefix_state_nums = {}) const;
        
        // either get frozen head view or throw
        std::shared_ptr<WorkspaceView> getFrozenWorkspaceHeadView() const;
        
        // stop all fixture threads - i.e. refresh and autocommit
        void stopThreads();
        
        // begin a new locked section
        // note that locked section IDs will be reused
        // @return the locked section ID (which must be used to end the section)
        unsigned int beginLocked();
        
        // End a specific locked section, callback will be notified with all mutated fixtures
        void endLocked(unsigned int, std::function<void(const std::string &prefix_name, std::uint64_t state_num)> callback);
                
    protected:
        friend class WorkspaceView;
        
        std::optional<std::uint64_t> getUUID(const PrefixName &) const;
        
    private:
        std::shared_ptr<Config> m_config;
        FixtureCatalog m_fixture_catalog;
        std::function<void(db0::swine_ptr<Fixture> &, bool, bool, bool)> m_fixture_initializer;
        // fixture by UUID
        std::unordered_map<std::uint64_t, db0::swine_ptr<Fixture> > m_fixtures;
        swine_ptr<db0::Fixture> m_default_fixture;
        std::vector<std::string> m_current_prefix_history;
        // shared object list is for maintainig v_object cache evition policy at a process level
        mutable FixedObjectList m_shared_object_list;
        // flag indicating atomic operation in progress
        AtomicContext *m_atomic_context_ptr = nullptr;
        mutable std::shared_ptr<LangCache> m_lang_cache;
        std::unique_ptr<WorkspaceThreads> m_workspace_threads;        
        // associated workspace views (some of which may already be deleted)
        mutable db0::weak_vector<WorkspaceView> m_views;
        // the designated "head" view with the prolonged lifetime
        mutable std::weak_ptr<WorkspaceView> m_head_view;
        std::function<void(db0::swine_ptr<Fixture> &, bool is_new)> m_on_open_callback;        
        unsigned int m_next_locked_section_id = 0;
        // active locked section IDs
        std::unordered_set<unsigned int> m_locked_section_ids;
        // log of prefixes closed inside locked sections
        std::unordered_map<unsigned int, std::vector<std::pair<std::string, std::uint64_t> > > m_locked_section_log;
        // this is to prevent recursive cleanups (which might result in a deadlock)
        mutable std::atomic<bool> m_cleanup_pending = false;
        
        void forEachMemspace(std::function<bool(Memspace &)> callback) override;
        
        // @return false if unable to handle this event at this time
        bool onCacheFlushed(bool threshold_reached) const override;

        void onFlushDirty(std::size_t limit) override;

        std::optional<std::size_t> getLangCacheSize() const;
        std::shared_ptr<WorkspaceView> getWorkspaceHeadView() const;        
    };
    
}