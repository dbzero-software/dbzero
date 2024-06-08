#include "Workspace.hpp"
#include <dbzero/core/memory/MetaAllocator.hpp>
#include "FixtureThreads.hpp"

namespace db0

{

    BaseWorkspace::BaseWorkspace(const std::string &root_path, std::optional<std::size_t> cache_size, std::optional<std::size_t> slab_cache_size)
        : m_prefix_catalog(root_path)
        , m_cache_recycler(cache_size ? *cache_size : DEFAULT_CACHE_SIZE)
        , m_slab_recycler(slab_cache_size ? *slab_cache_size : DEFAULT_SLAB_CACHE_SIZE)
    {
    }
    
    std::pair<std::shared_ptr<Prefix>, std::shared_ptr<MetaAllocator> > BaseWorkspace::openMemspace(const std::string &prefix_name,
        bool &new_file_created, AccessType access_type, std::optional<std::uint64_t> state_num, std::optional<std::size_t> page_size,
        std::optional<std::size_t> slab_size, std::optional<std::size_t> sparse_index_node_size)
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
        if (state_num && access_type != AccessType::READ_ONLY) {
            throw std::runtime_error("For historical states (time-travel) only the READ-ONLY access is allowed");
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
        auto prefix = std::make_shared<PrefixImpl<StorageT> >(prefix_name, m_cache_recycler, state_num,
            file_name, access_type);
        if (new_file_created) {
            // prepare meta allocator for the 1st use
            MetaAllocator::formatPrefix(prefix, *page_size, *slab_size);
        }
        auto allocator = std::make_shared<MetaAllocator>(prefix, &m_slab_recycler);
        return { prefix, allocator };
    }
    
    Memspace &BaseWorkspace::getMemspace(const std::string &prefix_name, AccessType access_type, std::optional<std::uint64_t> state_num,
        std::optional<std::size_t> page_size, std::optional<std::size_t> slab_size, 
        std::optional<std::size_t> sparse_index_node_size)
    {
        bool file_created = false;
        auto it = m_memspaces.find(prefix_name);
        try {
            if (it == m_memspaces.end()) {
                auto [prefix, allocator] = openMemspace(prefix_name, file_created, access_type, state_num, 
                    page_size, slab_size, sparse_index_node_size);
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
        auto it = m_memspaces.begin(), end = m_memspaces.end();
        while (it != end) {
            it->second.close();
            it = m_memspaces.erase(it);
        }
    }
    
    bool BaseWorkspace::drop(const std::string &prefix_name, bool if_exists)
    {
        close(prefix_name);
        return m_prefix_catalog.drop(prefix_name, if_exists);
    }
    
    Workspace::Workspace(const std::string &root_path, std::optional<std::size_t> cache_size, std::optional<std::size_t> slab_cache_size, 
        std::optional<std::size_t> vobject_cache_size, std::function<void(db0::swine_ptr<Fixture> &, bool)> fixture_initializer)
        : BaseWorkspace(root_path, cache_size, slab_cache_size)
        , m_fixture_catalog(m_prefix_catalog)
        , m_fixture_initializer(fixture_initializer)
        , m_refresh_thread(std::make_unique<RefreshThread>())
        , m_auto_commit_thread(std::make_unique<AutoCommitThread>(DEFAULT_AUTOCOMMIT_INTERVAL_MS))
        , m_shared_object_list(vobject_cache_size ? *vobject_cache_size : DEFAULT_VOBJECT_CACHE_SIZE)
    {        
        // run refresh / autocommit threads     
        m_threads.emplace_back([this]() {
            m_refresh_thread->run();
        });
        m_threads.emplace_back([this]() {
            m_auto_commit_thread->run();
        });        
    }
    
    Workspace::~Workspace()
    {
        // stop refresh/autocommit threads
        m_auto_commit_thread->stop();        
        m_refresh_thread->stop();
        for (auto &m_thread : m_threads) {
            m_thread.join();
        }        
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
        auto it = m_fixtures.begin(), end = m_fixtures.end();
        while (it != end) {
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

    swine_ptr<Fixture> Workspace::getFixtureEx(const std::string &prefix_name, std::optional<AccessType> access_type,
        std::optional<std::uint64_t> state_num, std::optional<std::size_t> page_size, std::optional<std::size_t> slab_size, 
        std::optional<std::size_t> sparse_index_node_size, bool autocommit)
    {
        bool file_created = false;
        auto uuid = getUUID(prefix_name);
        auto it = uuid ? m_fixtures.find(*uuid) : m_fixtures.end();
        try {
            if (it == m_fixtures.end()) {
                if (!access_type) {
                    THROWF(db0::InputException) << "Fixture with name " << prefix_name << " not found";
                }
                auto [prefix, allocator] = openMemspace(prefix_name, file_created, *access_type, state_num,
                    page_size, slab_size, sparse_index_node_size);
                if (file_created) {
                    // initialize new fixture
                    Fixture::formatFixture(Memspace(prefix, allocator), *allocator);
                }
                auto fixture = db0::make_swine<Fixture>(*this, prefix, allocator);
                if (m_fixture_initializer) {
                    // initialize fixture with a model-specific initializer
                    m_fixture_initializer(fixture, file_created);
                }
                it = m_fixtures.emplace(fixture->getUUID(), fixture).first;
                m_fixture_catalog.add(prefix_name, *fixture);
                if (*access_type == AccessType::READ_ONLY) {
                    // add fixture to be monitored by the refresh thread (will be removed automatically when closed)
                    m_refresh_thread->addFixture(fixture);
                }                
                if (*access_type == AccessType::READ_WRITE && autocommit) {
                    // register fixture for auto-commit
                    m_auto_commit_thread->addFixture(fixture);
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
            throw std::runtime_error("Upgrade to read/write access is not implemented");
        }

        return it->second;
    }
    
    swine_ptr<Fixture> Workspace::tryFindFixture(const std::string &prefix_name) const
    {
        auto uuid = getUUID(prefix_name);
        auto it = uuid ? m_fixtures.find(*uuid) : m_fixtures.end();        
        if (it == m_fixtures.end()) {
            return {};
        }
        return it->second;
    }
    
    swine_ptr<Fixture> Workspace::findFixture(const std::string &prefix_name) const
    {
        auto result = tryFindFixture(prefix_name);
        if (!result) {
            THROWF(db0::InputException) << "Fixture with name " << prefix_name << " not found";
        }
        return result;
    }
    
    swine_ptr<Fixture> Workspace::getFixture(std::uint64_t uuid, std::optional<AccessType> access_type)
    {
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
        
        return it->second;
    }
    
    std::optional<std::uint64_t> Workspace::getUUID(const std::string &prefix_name) const
    {
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

    std::function<void(db0::swine_ptr<Fixture> &, bool is_new)> Workspace::getFixtureInitializer() const
    {
        return m_fixture_initializer;
    }

    bool Workspace::drop(const std::string &prefix_name, bool if_exists)
    {
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

    FixtureLock Workspace::getMutableFixture() const
    {
        if (!m_default_fixture) {
            THROWF(db0::InternalException) << "DBZero: no default prefix exists";
        }
        m_default_fixture->onUpdated();
        return m_default_fixture;
    }
    
    db0::swine_ptr<Fixture> Workspace::getCurrentFixture(std::optional<AccessType> access_type)
    {
        if (!m_default_fixture) {
            THROWF(db0::InternalException) << "DBZero: no default prefix exists";
        }
        if (access_type && *access_type != AccessType::READ_ONLY) {
            THROWF(db0::InputException) << "DBZero: getCurrentFixture allows READ-ONLY access";
        }   
        return m_default_fixture;
    }
    
    void Workspace::open(const std::string &prefix_name, AccessType access_type, bool autocommit)
    {
        auto fixture = getFixtureEx(prefix_name, access_type, {}, {}, {}, {}, autocommit);
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
    
    swine_ptr<Fixture> Workspace::getFixture(const std::string &prefix_name, std::optional<AccessType> access_type) {
        return getFixtureEx(prefix_name, access_type);
    }
    
}