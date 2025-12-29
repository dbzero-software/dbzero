// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "Workspace.hpp"
#include <dbzero/core/memory/MetaAllocator.hpp>
#include "FixtureThreads.hpp"
#include "Config.hpp"
#include "WorkspaceView.hpp"
#include "PrefixName.hpp"
#include <thread>

namespace db0

{
    
    BaseWorkspace::BaseWorkspace(const std::string &root_path, std::optional<std::size_t> cache_size,
        std::optional<std::size_t> slab_cache_size, std::optional<std::size_t> flush_size,
        std::optional<LockFlags> default_lock_flags)
        : m_prefix_catalog(root_path)
        , m_default_lock_flags(default_lock_flags ? *default_lock_flags : LockFlags())        
        , m_cache_recycler(cache_size ? *cache_size : DEFAULT_CACHE_SIZE, m_dirty_meter, flush_size,
            [this](std::size_t limit) {
                this->onFlushDirty(limit);
            },
            [this](bool threshold_reached) -> bool {
                return this->onCacheFlushed(threshold_reached);
            }
        )
        , m_slab_recycler(slab_cache_size ? *slab_cache_size : DEFAULT_SLAB_CACHE_SIZE)
    {
    }
    
    std::pair<std::shared_ptr<Prefix>, std::shared_ptr<MetaAllocator> > BaseWorkspace::openMemspace(
        const PrefixName &prefix_name, bool &new_file_created, AccessType access_type, 
        std::optional<std::size_t> page_size, std::optional<std::size_t> slab_size, 
        std::optional<std::size_t> sparse_index_node_size, std::optional<LockFlags> lock_flags,
        std::optional<std::size_t> meta_io_step_size, std::optional<std::size_t> page_io_step_size)
    {
        if (!page_size) {
            page_size = DEFAULT_PAGE_SIZE;
        }
        if (!slab_size) {
            slab_size = DEFAULT_SLAB_SIZE;
        }
        if (!sparse_index_node_size) {
            sparse_index_node_size = DEFAULT_SPARSE_INDEX_NODE_SIZE;
        }
        
        new_file_created = false;
        auto file_name = m_prefix_catalog.getFileName(prefix_name).string();
        if (!m_prefix_catalog.exists(prefix_name)) {
            // create new file if READ-WRITE access permitted
            if (access_type == AccessType::READ_ONLY) {
                THROWF(db0::PrefixNotFoundException) << "Prefix does not exist: " << prefix_name;
            }
                        
            BDevStorage::create(file_name, *page_size, *sparse_index_node_size, page_io_step_size);
            new_file_created = true;
        }
        auto storage = std::make_shared<BDevStorage>(
            file_name, access_type, lock_flags ? *lock_flags : m_default_lock_flags, meta_io_step_size
        );
        auto prefix = std::make_shared<PrefixImpl>(
            prefix_name, m_dirty_meter, m_cache_recycler, storage
        );
        try {
            if (new_file_created) {
                // prepare meta allocator for the 1st use
                MetaAllocator::formatPrefix(prefix, *page_size, *slab_size);
            }
            auto allocator = std::make_shared<MetaAllocator>(prefix, &m_slab_recycler);
            return { prefix, allocator };
        } catch (...) {
            prefix->close();
            throw;
        }
    }
    
    bool BaseWorkspace::hasMemspace(const PrefixName &prefix_name) const {
        return m_prefix_catalog.exists(m_prefix_catalog.getFileName(prefix_name).string());
    }
    
    Memspace &BaseWorkspace::getMemspace(const PrefixName &prefix_name, AccessType access_type, 
        std::optional<std::size_t> page_size, std::optional<std::size_t> slab_size, 
        std::optional<std::size_t> sparse_index_node_size)
    {
        bool file_created = false;
        auto it = m_memspaces.find(prefix_name);
        try {
            if (it == m_memspaces.end()) {
                auto [prefix, allocator] = openMemspace(
                    prefix_name, file_created, access_type, page_size, slab_size, sparse_index_node_size
                );
                it = m_memspaces.emplace(prefix_name, Memspace(prefix, allocator)).first;
            }
        } catch (...) {
            if (file_created) {
                m_prefix_catalog.drop(prefix_name);
            }
            throw;
        }
        return it->second;
    }
    
    bool BaseWorkspace::commit()
    {   
        bool result = false;     
        for (auto &[prefix_name, memspace] : m_memspaces) {
            if (memspace.getAccessType() == AccessType::READ_WRITE) {
                result |= memspace.commit();
            }
        }
        return result;
    }
    
    bool BaseWorkspace::close(const PrefixName &prefix_name)
    {
        auto it = m_memspaces.find(prefix_name);
        if (it != m_memspaces.end()) {
            it->second.getPrefix().close();
            m_memspaces.erase(it);
            return true;
        }
        return false;
    }

    void BaseWorkspace::close(ProcessTimer *timer_ptr)
    {
        std::unique_ptr<ProcessTimer> timer;
        if (timer_ptr) {
            timer = std::make_unique<ProcessTimer>("BaseWorkspace::close", timer_ptr);
        }
        auto it = m_memspaces.begin();
        while (it != m_memspaces.end()) {
            it->second.close();
            it = m_memspaces.erase(it);
        }
    }
    
    bool BaseWorkspace::drop(const PrefixName &prefix_name, bool if_exists)
    {
        close(prefix_name);
        return m_prefix_catalog.drop(prefix_name);
    }
    
    void BaseWorkspace::setCacheSize(std::size_t new_cache_size) {
        m_cache_recycler.resize(new_cache_size);
    }
    
    void BaseWorkspace::clearCache() const {
        m_cache_recycler.clear();
    }

    bool BaseWorkspace::onCacheFlushed(bool) const {
        return true;
    }
    
    void BaseWorkspace::forEachMemspace(std::function<bool(Memspace &)> callback)
    {
        for (auto &[uuid, memspace] : m_memspaces) {
            if (!callback(memspace)) {
                break;
            }
        }
    }
    
    void BaseWorkspace::onFlushDirty(std::size_t limit)
    {
        // from each fixture try releasing limit proportional to its dirty size
        auto total_dirty_size = m_dirty_meter.load();
        // the implementation works for BaseWorkspace and its subclasses
        forEachMemspace([&](Memspace &memspace) -> bool {
            auto dirty_size = memspace.getPrefix().getDirtySize();
            if (dirty_size > 0) {
                auto p = (double)dirty_size / (double)total_dirty_size;
                auto size_flushed = memspace.getPrefix().flushDirty((std::size_t)(p * limit));
                // finish when limit is reached
                if (size_flushed > limit) {
                    return false;
                }
                total_dirty_size -= size_flushed;
                limit -= size_flushed;
            }            
            return true;
        });
    }

    class WorkspaceThreads
    {
    public:
        WorkspaceThreads()
            : m_auto_commit_thread(Workspace::DEFAULT_AUTOCOMMIT_INTERVAL_MS)
        {
            // run refresh / autocommit threads     
            m_threads.emplace_back([this]() {
                m_refresh_thread.run();
            });
            m_threads.emplace_back([this]() {
                m_auto_commit_thread.run();
            });        
        }

        ~WorkspaceThreads()
        {
            // stop refresh/autocommit threads            
            m_auto_commit_thread.stop();
            m_refresh_thread.stop();
            for (auto &m_thread : m_threads) {
                m_thread.join();
            }
        }

        void startRefresh(db0::swine_ptr<Fixture> &fixture) {
            m_refresh_thread.addFixture(fixture);
        }

        void startAutoCommit(db0::swine_ptr<Fixture> &fixture) {
            m_auto_commit_thread.addFixture(fixture);
        }

        void setAutocommitInterval(std::uint64_t interval_ms) {            
            m_auto_commit_thread.setInterval(interval_ms);
        }

    private:
        std::vector<std::thread> m_threads;
        RefreshThread m_refresh_thread;
        AutoCommitThread m_auto_commit_thread;
    };

    Workspace::Workspace(const std::string &root_path, std::optional<std::size_t> cache_size, 
        std::optional<std::size_t> slab_cache_size, std::optional<std::size_t> vobject_cache_size, 
        std::optional<std::size_t> flush_size, std::function<void(db0::swine_ptr<Fixture> &, bool, bool, bool)> fixture_initializer,
        std::shared_ptr<Config> config, std::optional<LockFlags> default_lock_flags)
        : BaseWorkspace(root_path, cache_size, slab_cache_size, flush_size, 
            default_lock_flags)
        , m_config(config)
        , m_fixture_catalog(m_prefix_catalog)
        , m_fixture_initializer(fixture_initializer)
        , m_shared_object_list(vobject_cache_size ? *vobject_cache_size : DEFAULT_VOBJECT_CACHE_SIZE)
        , m_lang_cache(std::make_shared<LangCache>(getLangCacheSize()))
        , m_workspace_threads(std::make_unique<WorkspaceThreads>())        
    {
        // apply autocommit interval if configured
        std::optional<unsigned long long> autocommit_interval_ms = (m_config ? m_config->get<unsigned long long>("autocommit_interval") : std::nullopt);
        if (autocommit_interval_ms) {
            this->setAutocommitInterval(*autocommit_interval_ms);
        }
    }
    
    Workspace::~Workspace()
    {
    }
    
    bool Workspace::close(const PrefixName &prefix_name)
    {
        BaseWorkspace::close(prefix_name);
        auto uuid = getUUID(prefix_name);
        if (uuid) {
            auto it = m_fixtures.find(*uuid);
            if (it != m_fixtures.end()) {
                auto &fixture = *(it->second);
                std::unordered_set<unsigned int> callback_log;
                fixture.endAllLocked([&](unsigned int locked_section_id) {
                    if (callback_log.find(locked_section_id) == callback_log.end()) {
                        // log prefixes closed inside the locked section
                        m_locked_section_log[locked_section_id].emplace_back(fixture.getPrefix().getName(), fixture.getStateNum());
                        callback_log.insert(locked_section_id);
                    }
                });
                                
                bool is_default = (it->second == m_default_fixture);
                it->second->close(false);
                m_fixtures.erase(it);

                if (is_default) {
                    m_default_fixture = {};
                    assert(!m_current_prefix_history.empty());
                    m_current_prefix_history.pop_back();
                    // change default fixture to the last one from the history
                    while (!m_current_prefix_history.empty() && !m_default_fixture) {
                        m_default_fixture = tryFindFixture(m_current_prefix_history.back());
                        m_current_prefix_history.pop_back();
                    }
                }

                return true;
            }
        }
        return false;
    }
    
    void Workspace::stopThreads() {
        m_workspace_threads = nullptr;
    }
    
    void Workspace::close(bool as_defunct, ProcessTimer *timer_ptr)
    {
        std::unique_ptr<ProcessTimer> timer;
        if (timer_ptr) {
            timer = std::make_unique<ProcessTimer>("Workspace::close", timer_ptr);
        }
        
        // close associated workspace views
        m_views.forEach([as_defunct](WorkspaceView &view) {
            view.close(as_defunct);
        });

        m_views.clear();
        // stop all workspace threads first
        stopThreads();
        m_shared_object_list.clear();
        auto it = m_fixtures.begin();
        while (it != m_fixtures.end()) {
            it->second->close(as_defunct, timer.get());
            it = m_fixtures.erase(it);
        }
        
        if (as_defunct) {
            m_lang_cache->clearDefunct();
        }

        m_default_fixture = {};
        m_current_prefix_history.clear();
        BaseWorkspace::close(timer.get());
    }
    
    CacheRecycler &Workspace::getCacheRecycler() {
        return BaseWorkspace::getCacheRecycler();
    }

    const CacheRecycler &Workspace::getCacheRecycler() const {
        return BaseWorkspace::getCacheRecycler();
    }
    
    db0::swine_ptr<Fixture> Workspace::tryGetFixtureEx(const PrefixName &prefix_name,
        std::optional<AccessType> access_type, std::optional<std::size_t> page_size, 
        std::optional<std::size_t> slab_size, std::optional<std::size_t> sparse_index_node_size, 
        std::optional<bool> autocommit, std::optional<LockFlags> lock_flags, std::optional<std::size_t> meta_io_step_size,
        std::optional<std::size_t> page_io_step_size)
    {
        bool file_created = false;
        auto uuid = getUUID(prefix_name);
        auto it = uuid ? m_fixtures.find(*uuid) : m_fixtures.end();
        if (!autocommit && m_config) {
            autocommit = m_config->get<bool>("autocommit");
        }
        try {
            if (it == m_fixtures.end()) {
                if (!access_type) {
                    return nullptr;                    
                }
                bool read_only = (*access_type == AccessType::READ_ONLY);
                auto [prefix, allocator] = openMemspace(prefix_name, file_created, *access_type, page_size, slab_size, 
                    sparse_index_node_size, lock_flags, meta_io_step_size, page_io_step_size
                );
                if (file_created) {
                    // initialize new fixture
                    Fixture::formatFixture(Memspace(prefix, allocator), *allocator);
                }
                auto fixture = db0::make_swine<Fixture>(*this, prefix, allocator, m_next_locked_section_id);
                if (m_fixture_initializer) {
                    // initialize fixture with a model-specific initializer
                    m_fixture_initializer(fixture, file_created, read_only, false);
                }
                
                if (file_created) {
                    // finalize fixture initialization and end the first transaction
                    fixture->commit();
                }
                
                if (m_atomic_context_ptr && *access_type == AccessType::READ_WRITE) {
                    // begin atomic with the new read/write fixture
                    fixture->beginAtomic(m_atomic_context_ptr);
                }
                
                it = m_fixtures.emplace(fixture->getUUID(), fixture).first;
                m_fixture_catalog.add(prefix_name, *fixture);
                if (*access_type == AccessType::READ_ONLY) {
                    // add read-only fixture to be monitored by the refresh thread (will be removed automatically when closed)
                    m_workspace_threads->startRefresh(fixture);
                }
                if (*access_type == AccessType::READ_WRITE && autocommit.value_or(true)) {
                    // register fixture for auto-commit
                    m_workspace_threads->startAutoCommit(fixture);
                }

                // complete fixture initialization
                if (m_on_open_callback) {
                    m_on_open_callback(it->second, file_created);
                }
            }
        } catch (db0::PrefixNotFoundException &ex) {
            if (file_created) {
                // remove incomplete file
                m_fixture_catalog.drop(prefix_name);                
            }
            return nullptr;         
        }
        catch (std::exception &ex) {
            if (file_created) {
                // remove incomplete file
                m_fixture_catalog.drop(prefix_name);                
            }
            throw;         
        }
        
        // Validate access type
        if (access_type && *access_type == AccessType::READ_WRITE && it->second->getAccessType() != AccessType::READ_WRITE) {
            // try upgrading access to read/write or fail
            // FIXME: implement
            // throw std::runtime_error("Upgrade to read/write access is not implemented");
        }
        
        return it->second;
    }
    
    swine_ptr<Fixture> Workspace::getFixtureEx(const PrefixName &px_name, std::optional<AccessType> access_type,
        std::optional<std::size_t> page_size, std::optional<std::size_t> slab_size, 
        std::optional<std::size_t> sparse_index_node_size,
        std::optional<bool> autocommit, std::optional<LockFlags> lock_flags,
        std::optional<std::size_t> meta_io_step_size, std::optional<std::size_t> page_io_step_size)
    {
        auto fixture = tryGetFixtureEx(px_name, access_type, page_size, slab_size, sparse_index_node_size,
            autocommit, lock_flags, meta_io_step_size, page_io_step_size
        );
        if (!fixture) {
            THROWF(db0::InputException) << "Prefix: " << px_name << " not found";
        }
        return fixture;
    }
    
    bool Workspace::hasFixture(const PrefixName &prefix_name) const
    {        
        auto uuid = getUUID(prefix_name);
        auto it = uuid ? m_fixtures.find(*uuid) : m_fixtures.end();
        if (it != m_fixtures.end()) {
            return true;
        }

        return hasMemspace(prefix_name);
    }
    
    db0::swine_ptr<Fixture> Workspace::tryFindFixture(const PrefixName &prefix_name) const
    {
        auto uuid = getUUID(prefix_name);
        auto it = uuid ? m_fixtures.find(*uuid) : m_fixtures.end();        
        if (it == m_fixtures.end()) {
            return {};
        }
        return it->second;
    }
    
    db0::swine_ptr<Fixture> Workspace::tryGetFixture(std::uint64_t uuid, std::optional<AccessType> access_type)
    {
        db0::swine_ptr<Fixture> result;
        if (uuid) {
            auto it = m_fixtures.find(uuid);
            if (it == m_fixtures.end()) {
                if (!access_type) {
                    return nullptr;
                }
                m_fixture_catalog.refresh();
                auto maybe_prefix_name = m_fixture_catalog.getPrefixName(uuid);
                if (!maybe_prefix_name) {
                    return nullptr;
                }
                // try opening fixture by name
                return getFixtureEx(*maybe_prefix_name, *access_type);
            }
            result = it->second;
        } else {
            result = getCurrentFixture();
        }
        
        assureAccessType(*result, access_type);
        return result;
    }
    
    std::optional<std::uint64_t> Workspace::getUUID(const PrefixName &prefix_name) const {
        return m_fixture_catalog.getFixtureUUID(prefix_name);
    }
    
    bool Workspace::commit()
    {
        bool result = false;
        for (auto &[uuid, fixture] : m_fixtures) {
            if (fixture->getAccessType() == AccessType::READ_WRITE) {
                result |= fixture->commit();
            }
        }
        if (result) {
            // invalidate the head view
            m_head_view = {};
        }
        return result;
    }
    
    void Workspace::flush()
    {
        for (auto &[uuid, fixture] : m_fixtures) {
            if (fixture->getAccessType() == AccessType::READ_WRITE) {
                fixture->flush();
            }
        }
    }

    bool Workspace::refresh(bool if_updated)
    {        
        bool refreshed = false;
        for (auto &[uuid, fixture] : m_fixtures) {
            // only makes sense to refresh read-only fixtures
            if (fixture->getAccessType() == AccessType::READ_ONLY) {
                refreshed |= if_updated ? fixture->refreshIfUpdated() : fixture->refresh();
            }
        }
        return refreshed;
    }
    
    void Workspace::forEachFixture(std::function<bool(const Fixture &)> callback) const
    {
        for (auto &[uuid, fixture] : m_fixtures) {
            if (!callback(*fixture)) {
                return;
            }
        }
    }
    
    std::function<void(db0::swine_ptr<Fixture> &, bool is_new, bool is_read_only, bool is_snapshot)>
    Workspace::getFixtureInitializer() const {
        return m_fixture_initializer;
    }
    
    bool Workspace::drop(const PrefixName &prefix_name, bool if_exists) {
        return BaseWorkspace::drop(prefix_name, if_exists);
    }

    bool Workspace::commit(const PrefixName &prefix_name)
    {
        auto fixture = findFixture(prefix_name);
        if (!fixture) {
            THROWF(db0::InputException) << "Prefix: " << prefix_name << " not found";
        }        
        if (fixture->getAccessType() != AccessType::READ_WRITE) {
            THROWF(db0::InputException) << "Prefix: " << prefix_name << " is not opened as read/write";
        }         
        auto result = fixture->commit();
        if (result) {
            // invalidate the head view
            m_head_view = {};
        }
        return result;
    }
    
    db0::swine_ptr<Fixture> Workspace::getCurrentFixture()
    {
        if (!m_default_fixture) {
            THROWF(db0::InternalException) << "dbzero: no default prefix exists";
        }
        return m_default_fixture;
    }
    
    void Workspace::open(const PrefixName &prefix_name, AccessType access_type, std::optional<bool> autocommit,
        std::optional<std::size_t> slab_size, std::optional<LockFlags> lock_flags, 
        std::optional<std::size_t> meta_io_step_size, std::optional<std::size_t> page_io_step_size)
    {
        auto fixture = getFixtureEx(prefix_name, access_type, {}, slab_size, {}, autocommit, 
            lock_flags, meta_io_step_size, page_io_step_size
        );
        // update default fixture
        if (!m_default_fixture || (*m_default_fixture != *fixture)) {
            m_default_fixture = fixture;
            m_current_prefix_history.push_back(prefix_name);
        }
    }
    
    FixedObjectList &Workspace::getSharedObjectList() const {
        return m_shared_object_list;
    }
    
    std::optional<std::uint64_t> Workspace::getDefaultUUID() const
    {
        if (!m_default_fixture) {
            return {};
        }
        return m_default_fixture->getUUID();
    }
    
    db0::swine_ptr<Fixture> Workspace::tryGetFixture(const PrefixName &prefix_name, std::optional<AccessType> access_type) {
        return tryGetFixtureEx(prefix_name, access_type);
    }
    
    void Workspace::preAtomic()
    {
        assert(!m_atomic_context_ptr);
        // begin atomic with all open read/write fixtures
        for (auto &[uuid, fixture] : m_fixtures) {
            if (fixture->getAccessType() == AccessType::READ_WRITE) {
                fixture->preAtomic();
            }
        }
    }
    
    void Workspace::beginAtomic(AtomicContext *context)
    {
        assert(!m_atomic_context_ptr);
        // begin atomic with all open read/write fixtures
        for (auto &[uuid, fixture] : m_fixtures) {
            if (fixture->getAccessType() == AccessType::READ_WRITE) {
                fixture->beginAtomic(context);
            }
        }
        m_atomic_context_ptr = context;
    }
    
    unsigned int Workspace::beginLocked()
    {
        auto locked_section_id = m_next_locked_section_id++;
        m_locked_section_ids.insert(locked_section_id);
        for (auto &[uuid, fixture] : m_fixtures) {
            if (fixture->getAccessType() == AccessType::READ_WRITE) {
                fixture->beginLocked(locked_section_id);
            }
        }
        return locked_section_id;
    }
    
    void Workspace::endLocked(unsigned int locked_section_id, std::function<void(const std::string &, std::uint64_t)> callback)
    {
        std::unordered_set<std::string> px_names;
        for (auto &[uuid, fixture] : m_fixtures) {
            if (fixture->getAccessType() == AccessType::READ_WRITE) {
                if (fixture->endLocked(locked_section_id)) {
                    auto px_name = fixture->getPrefix().getName();
                    // avoid reporting more than once
                    if (px_names.insert(px_name).second) {
                        callback(px_name, fixture->getStateNum());
                    }
                }
            }
        }
        
        // also report prefixes closed inside the locked section
        auto it = m_locked_section_log.find(locked_section_id);
        if (it != m_locked_section_log.end()) {
            for (auto &[prefix_name, state_num] : it->second) {
                // avoid reporting more than once
                if (px_names.insert(prefix_name).second) {
                    callback(prefix_name, state_num);
                }
            }
            m_locked_section_log.erase(it);
        }
        
        m_locked_section_ids.erase(locked_section_id);
        // reuse locked section IDs to keep the assigned values low
        while (m_next_locked_section_id > 0 &&
            m_locked_section_ids.find(m_next_locked_section_id - 1) == m_locked_section_ids.end())
        {
            --m_next_locked_section_id;
        }
    }
    
    void Workspace::detach()
    {        
        // detach mutable fixtures only (as a preparation step before endAtomic)
        for (auto &[uuid, fixture] : m_fixtures) {
            if (fixture->getAccessType() == AccessType::READ_WRITE) {
                fixture->detach();
            }
        }        
    }
    
    void Workspace::endAtomic()
    {
        assert(m_atomic_context_ptr);
        // end atomic with all open fixtures
        for (auto &[uuid, fixture] : m_fixtures) {
            if (fixture->getAccessType() == AccessType::READ_WRITE) {
                fixture->endAtomic();
            }
        }
        m_atomic_context_ptr = nullptr;
    }
    
    void Workspace::cancelAtomic()
    {
        assert(m_atomic_context_ptr);
        // end atomic with all open fixtures
        for (auto &[uuid, fixture] : m_fixtures) {
            fixture->cancelAtomic();
        }
        m_atomic_context_ptr = nullptr;
    }

    void Workspace::setAutocommitInterval(std::uint64_t interval_ms) {
        m_workspace_threads->setAutocommitInterval(interval_ms);        
    }

    void Workspace::setCacheSize(std::size_t cache_size) {
        BaseWorkspace::setCacheSize(cache_size);
    }
    
    std::shared_ptr<LangCache> Workspace::getLangCache() const {
        return m_lang_cache;
    }
    
    void Workspace::clearCache() const
    {
        BaseWorkspace::clearCache();
        // remove expired only objects
        m_lang_cache->clear(true);
    }

    void Workspace::onFlushDirty(std::size_t limit) {
        BaseWorkspace::onFlushDirty(limit);
    }
    
    bool Workspace::onCacheFlushed(bool threshold_reached) const
    {
        // prevent recursive cleanups
        if (m_cleanup_pending) {
            return false;
        }
        m_cleanup_pending = true;
        try {
            if (!BaseWorkspace::onCacheFlushed(threshold_reached)) {
                return false;
            }
            for (auto &[uuid, fixture] : m_fixtures) {
                if (!fixture->onCacheFlushed(threshold_reached)) {
                    // unable to handle this event - e.g. due to pending commit
                    return false;
                }
            }
            
            if (!threshold_reached) {
                // additionally erase the entire LangCache to attempt reaching the flush objective            
                m_lang_cache->clear(true);
            }
        } catch (...) {
            m_cleanup_pending = false;
            throw;
        }
        m_cleanup_pending = false;
        return true;
    }
    
    const FixtureCatalog &Workspace::getFixtureCatalog() const {
        return m_fixture_catalog;
    }
    
    void Workspace::setOnOpenCallback(std::function<void(db0::swine_ptr<Fixture> &, bool is_new)> callback) {
        m_on_open_callback = callback;
    }
    
    std::shared_ptr<WorkspaceView> Workspace::getWorkspaceView(std::optional<std::uint64_t> state_num,
        const std::unordered_map<std::string, std::uint64_t> &prefix_state_nums) const
    {
        // the head view has a special handling and prolonged scope
        if (!state_num && prefix_state_nums.empty()) {
            return getWorkspaceHeadView();
        }
        
        auto workspace_view = std::shared_ptr<WorkspaceView>(
            new WorkspaceView(const_cast<Workspace&>(*this), state_num, prefix_state_nums)
        );
        m_views.push_back(workspace_view);
        return workspace_view;
    }
    
    std::shared_ptr<WorkspaceView> Workspace::getFrozenWorkspaceHeadView() const
    {
        auto result = m_head_view.lock();
        if (!result) {
            THROWF(db0::InputException) << "Frozen head snapshot not available";
        }
        return result;
    }
    
    std::shared_ptr<WorkspaceView> Workspace::getWorkspaceHeadView() const
    {
        auto result = m_head_view.lock();
        if (!result) {
            result = std::shared_ptr<WorkspaceView>(new WorkspaceView(const_cast<Workspace&>(*this)));
            m_head_view = result;
            m_views.push_back(result);
        }
        return result;
    }
    
    void Workspace::forEachMemspace(std::function<bool(Memspace &)> callback)
    {
        for (auto &[uuid, fixture] : m_fixtures) {
            if (!callback(*fixture)) {
                break;
            }
        }
    }
    
    bool Workspace::isMutable() const {
        return true;
    }
        
    std::size_t Workspace::size() const {
        return m_fixtures.size();
    }
    
    std::optional<std::size_t> Workspace::getLangCacheSize() const
    {
        if (m_config) {
            auto value = m_config->get<unsigned long long>("lang_cache_size");
            if (value) {
                return value;
            }
        }
        return std::nullopt;
    }
    
}