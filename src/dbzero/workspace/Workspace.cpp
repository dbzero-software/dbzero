#include "Workspace.hpp"
#include <dbzero/core/memory/MetaAllocator.hpp>
#include "FixtureThreads.hpp"
#include "Config.hpp"
#include "WorkspaceView.hpp"
#include <thread>

namespace db0

{

    void validateAccessType(AccessType requested, AccessType actual)
    {
        if (requested == AccessType::READ_WRITE && actual != AccessType::READ_WRITE) {
            THROWF(db0::InputException) << "Unable to update the read-only prefix";
        }
    }
    
    BaseWorkspace::BaseWorkspace(const std::string &root_path, std::optional<std::size_t> cache_size,
        std::optional<std::size_t> slab_cache_size, std::optional<std::size_t> flush_size)
        : m_prefix_catalog(root_path)
        , m_cache_recycler(cache_size ? *cache_size : DEFAULT_CACHE_SIZE, flush_size, [this](bool threshold_reached) {
            this->onCacheFlushed(threshold_reached);
        })
        , m_slab_recycler(slab_cache_size ? *slab_cache_size : DEFAULT_SLAB_CACHE_SIZE)
    {
    }
    
    std::pair<std::shared_ptr<Prefix>, std::shared_ptr<MetaAllocator> > BaseWorkspace::openMemspace(const std::string &prefix_name,
        bool &new_file_created, AccessType access_type, std::optional<std::size_t> page_size, std::optional<std::size_t> slab_size, 
        std::optional<std::size_t> sparse_index_node_size)
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
                THROWF(db0::InputException) << "Prefix does not exist: " << prefix_name;
            }
            
            StorageT::create(file_name, *page_size, *sparse_index_node_size);
            new_file_created = true;
        }
        auto prefix = std::make_shared<PrefixImpl<StorageT> >(prefix_name, m_cache_recycler, file_name, access_type);
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
    
    bool BaseWorkspace::hasMemspace(const std::string &prefix_name) const {
        return m_prefix_catalog.exists(m_prefix_catalog.getFileName(prefix_name).string());
    }

    Memspace &BaseWorkspace::getMemspace(const std::string &prefix_name, AccessType access_type, 
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
    
    void BaseWorkspace::commit()
    {        
        for (auto &[prefix_name, memspace] : m_memspaces) {
            if (memspace.getAccessType() == AccessType::READ_WRITE) {
                memspace.commit();
            }            
        }    
    }

    bool BaseWorkspace::close(const std::string &prefix_name)
    {
        auto it = m_memspaces.find(prefix_name);
        if (it != m_memspaces.end()) {
            it->second.getPrefix().close();
            m_memspaces.erase(it);
            return true;
        }
        return false;
    }
    
    void BaseWorkspace::close()
    {
        auto it = m_memspaces.begin();
        while (it != m_memspaces.end()) {
            it->second.close();
            it = m_memspaces.erase(it);
        }
    }
    
    bool BaseWorkspace::drop(const std::string &prefix_name, bool if_exists)
    {
        close(prefix_name);
        return m_prefix_catalog.drop(prefix_name, if_exists);
    }
    
    void BaseWorkspace::setCacheSize(std::size_t new_cache_size) {
        m_cache_recycler.resize(new_cache_size);
    }
    
    void BaseWorkspace::clearCache() const {
        m_cache_recycler.clear();
    }
    
    void BaseWorkspace::onCacheFlushed(bool) const
    {
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
        std::optional<std::size_t> flush_size, std::function<void(db0::swine_ptr<Fixture> &, bool, bool)> fixture_initializer,
        std::shared_ptr<Config> config)
        : BaseWorkspace(root_path, cache_size, slab_cache_size, flush_size)
        , m_fixture_catalog(m_prefix_catalog)
        , m_fixture_initializer(fixture_initializer)
        , m_shared_object_list(vobject_cache_size ? *vobject_cache_size : DEFAULT_VOBJECT_CACHE_SIZE)
        , m_lang_cache(std::make_shared<LangCache>())
        , m_workspace_threads(std::make_unique<WorkspaceThreads>())
        , m_config(config)
    {
        // apply autocommit interval if configured
        std::optional<long> autocommit_interval_ms = (m_config ? m_config->get<long>("autocommit_interval") : std::nullopt);
        if (autocommit_interval_ms) {
            this->setAutocommitInterval(*autocommit_interval_ms);
        }
    }
    
    Workspace::~Workspace()
    {    
    }

    bool Workspace::close(const std::string &prefix_name)
    {
        BaseWorkspace::close(prefix_name);
        auto uuid = getUUID(prefix_name);
        if (uuid) {
            auto it = m_fixtures.find(*uuid);
            if (it != m_fixtures.end()) {
                bool is_default = (it->second == m_default_fixture);
                it->second->close();
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
    
    void Workspace::close()
    {        
        // close associated workspace views
        for (auto &view_ptr : m_views) {
            if (auto ptr = view_ptr.lock()) {
                ptr->close();
            }
        }
        m_views.clear();
        // stop all workspace threads first
        m_workspace_threads = nullptr;
        m_shared_object_list.clear();
        auto it = m_fixtures.begin();
        while (it != m_fixtures.end()) {
            it->second->close();
            it = m_fixtures.erase(it);
        }
        m_default_fixture = {};
        m_current_prefix_history.clear();
        BaseWorkspace::close();
    }
    
    CacheRecycler &Workspace::getCacheRecycler() {
        return BaseWorkspace::getCacheRecycler();
    }

    const CacheRecycler &Workspace::getCacheRecycler() const {
        return BaseWorkspace::getCacheRecycler();
    }
    
    db0::swine_ptr<Fixture> Workspace::getFixtureEx(const std::string &prefix_name, std::optional<AccessType> access_type,
        std::optional<std::size_t> page_size, std::optional<std::size_t> slab_size, 
        std::optional<std::size_t> sparse_index_node_size, std::optional<bool> autocommit)
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
                    THROWF(db0::InputException) << "Fixture with name " << prefix_name << " not found";
                }
                bool read_only = (*access_type == AccessType::READ_ONLY);
                auto [prefix, allocator] = openMemspace(
                    prefix_name, file_created, *access_type, page_size, slab_size, sparse_index_node_size
                );
                if (file_created) {
                    // initialize new fixture
                    Fixture::formatFixture(Memspace(prefix, allocator), *allocator);
                }
                auto fixture = db0::make_swine<Fixture>(*this, prefix, allocator);
                if (m_fixture_initializer) {
                    // initialize fixture with a model-specific initializer
                    m_fixture_initializer(fixture, file_created, read_only);
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
        } catch (...) {
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
    
    bool Workspace::hasFixture(const std::string &prefix_name) const
    {        
        auto uuid = getUUID(prefix_name);
        auto it = uuid ? m_fixtures.find(*uuid) : m_fixtures.end();
        if (it != m_fixtures.end()) {
            return true;
        }

        return hasMemspace(prefix_name);
    }
    
    db0::swine_ptr<Fixture> Workspace::tryFindFixture(const std::string &prefix_name) const
    {
        auto uuid = getUUID(prefix_name);
        auto it = uuid ? m_fixtures.find(*uuid) : m_fixtures.end();        
        if (it == m_fixtures.end()) {
            return {};
        }
        return it->second;
    }
    
    db0::swine_ptr<Fixture> Workspace::findFixture(const std::string &prefix_name) const
    {
        auto result = tryFindFixture(prefix_name);
        if (!result) {
            THROWF(db0::InputException) << "Fixture with name " << prefix_name << " not found";
        }
        return result;
    }
    
    db0::swine_ptr<Fixture> Workspace::getFixture(std::uint64_t uuid, std::optional<AccessType> access_type)
    {
        db0::swine_ptr<Fixture> result;
        if (uuid) {
            auto it = m_fixtures.find(uuid);
            if (it == m_fixtures.end()) {
                if (!access_type) {
                    THROWF(db0::InputException) << "Fixture with UUID " << uuid << " not found";
                }
                m_fixture_catalog.refresh();
                auto maybe_prefix_name = m_fixture_catalog.getPrefixName(uuid);
                if (!maybe_prefix_name) {
                    THROWF(db0::InputException) << "Fixture with UUID " << uuid << " not found";
                }
                // try opening fixture by name                
                return getFixtureEx(*maybe_prefix_name, *access_type);
            }
            result = it->second;
        } else {
            result = getCurrentFixture();
        }
        
        if (access_type) {
            validateAccessType(*access_type, result->getAccessType());
        }
        return result;
    }
    
    std::optional<std::uint64_t> Workspace::getUUID(const std::string &prefix_name) const {
        return m_fixture_catalog.getFixtureUUID(prefix_name);
    }
    
    void Workspace::commit()
    {
        for (auto &[uuid, fixture] : m_fixtures) {
            if (fixture->getAccessType() == AccessType::READ_WRITE) {
                fixture->commit();
            }
        }
    }

    bool Workspace::refresh()
    {
        bool refreshed = false;
        for (auto &[uuid, fixture] : m_fixtures) {
            if (fixture->refresh()) {
                refreshed = true;
            }
        }
        return refreshed;
    }
    
    void Workspace::forAll(std::function<void(const Fixture &)> callback) const
    {
        for (auto &[uuid, fixture] : m_fixtures) {
            callback(*fixture);
        }
    }

    std::function<void(db0::swine_ptr<Fixture> &, bool is_new, bool is_read_only)>
    Workspace::getFixtureInitializer() const {
        return m_fixture_initializer;
    }

    bool Workspace::drop(const std::string &prefix_name, bool if_exists) {
        return BaseWorkspace::drop(prefix_name, if_exists);
    }
    
    void Workspace::commit(const std::string &prefix_name)
    {
        auto fixture = findFixture(prefix_name);
        if (!fixture) {
            THROWF(db0::InputException) << "Prefix: " << prefix_name << " not found";
        }        
        if (fixture->getAccessType() != AccessType::READ_WRITE) {
            THROWF(db0::InputException) << "Prefix: " << prefix_name << " is not opened as read/write";
        }         
        fixture->commit();
    }
    
    db0::swine_ptr<Fixture> Workspace::getCurrentFixture()
    {
        if (!m_default_fixture) {
            THROWF(db0::InternalException) << "DBZero: no default prefix exists";
        }
        return m_default_fixture;
    }
    
    void Workspace::open(const std::string &prefix_name, AccessType access_type, std::optional<bool> autocommit,
        std::optional<std::size_t> slab_size)
    {
        auto fixture = getFixtureEx(prefix_name, access_type, {}, slab_size, {}, autocommit);
        // update default fixture
        if (!m_default_fixture || (m_default_fixture->getAccessType() <= access_type)) {
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
    
    db0::swine_ptr<Fixture> Workspace::getFixture(const std::string &prefix_name, std::optional<AccessType> access_type) {
        return getFixtureEx(prefix_name, access_type);
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
    
    void Workspace::onCacheFlushed(bool threshold_reached) const
    {
        BaseWorkspace::onCacheFlushed(threshold_reached);
        if (!threshold_reached) {
            // additionally erase the entire LangCache to attempt reaching the flush objective
            m_lang_cache->clear(true);
        }
        for (auto &[uuid, fixture] : m_fixtures) {
            fixture->onCacheFlushed(threshold_reached);
        }
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
        // clean-up expired views
        m_views.remove_if([](const std::weak_ptr<WorkspaceView> &view) {
            return view.expired();
        });
        auto workspace_view = std::shared_ptr<WorkspaceView>(
            new WorkspaceView(const_cast<Workspace&>(*this), state_num, prefix_state_nums));
        m_views.push_back(workspace_view);
        return workspace_view;
    }

}