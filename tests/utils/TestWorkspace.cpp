#include "TestWorkspace.hpp"
#include "EmbeddedAllocator.hpp"

#include <dbzero/core/memory/PrefixImpl.hpp>
#include <dbzero/core/memory/MetaAllocator.hpp>
#include <dbzero/core/storage/Storage0.hpp>

namespace db0

{
    
    TestWorkspaceBase::TestWorkspaceBase(std::size_t page_size, std::size_t cache_size)
        : m_page_size(page_size)
        , m_cache_recycler(cache_size, m_dirty_meter)
    {
    }
    
    TestWorkspaceBase::~TestWorkspaceBase()
    {
        if (m_prefix) {
            m_prefix->close();
        }
    }
    
    Memspace TestWorkspaceBase::getMemspace(const PrefixName &name, AllocCallbackT callback)
    {
        using PrefixT = PrefixImpl<db0::Storage0>;
        if (!m_prefix) {
            auto prefix = std::shared_ptr<Prefix>(new PrefixT(name, m_dirty_meter, m_cache_recycler, m_page_size));
            m_prefix = std::make_shared<db0::tests::PrefixProxy>(prefix);
        }
        auto allocator = std::make_shared<EmbeddedAllocator>();
        if (callback) {
            allocator->setAllocCallback(callback);
        }
        return { m_prefix, allocator, TEST_MEMSPACE_UUID };
    }
    
    void TestWorkspaceBase::tearDown()
    {
        if (m_prefix) {
            m_prefix->tearDown();
        }        
    }

    void TestWorkspaceBase::setMapRangeCallback(std::function<void(std::uint64_t, std::size_t, FlagSet<AccessOptions>)> callback)
    {
        if (m_prefix) {
            m_prefix->setMapRangeCallback(callback);
        }
    }
    
    bool TestWorkspace::close(const PrefixName &name)
    {
        auto it_uuid = m_uuids.find(name);
        if (it_uuid == m_uuids.end()) {
            return false;            
        }

        auto it = m_fixtures.find(it_uuid->second);
        if (it != m_fixtures.end()) {
            it->second->close();
            m_fixtures.erase(it);
            return true;
        }
        return false;
    }
    
    void TestWorkspace::close()
    {
        m_current_fixture = nullptr;
        for (auto &fixture: m_fixtures) {
            fixture.second->close();
        }
        m_fixtures.clear();        
    }

    TestWorkspace::TestWorkspace(std::size_t page_size, std::size_t slab_size, std::size_t cache_size)
        : TestWorkspaceBase(page_size, cache_size)
        , m_slab_size(slab_size)
        , m_shared_object_list(100)
        , m_lang_cache(std::make_shared<LangCache>())
    {
    }

    db0::swine_ptr<Fixture> TestWorkspace::getFixture(const PrefixName &prefix_name, std::optional<AccessType> access_type)
    {
        auto it = m_uuids.find(prefix_name);
        if (it != m_uuids.end()) {
            return getFixture(it->second, access_type);
        }
        using PrefixT = PrefixImpl<db0::Storage0>;
        auto prefix = std::shared_ptr<Prefix>(new PrefixT(prefix_name, m_dirty_meter, m_cache_recycler, m_page_size));
        auto proxy = std::make_shared<db0::tests::PrefixProxy>(prefix);
        // prepare meta allocator for the 1st use
        MetaAllocator::formatPrefix(prefix, m_page_size, m_slab_size);
        auto meta = std::make_shared<MetaAllocator>(proxy, &m_slab_recycler);
        // prepare fixture to first use
        {
            Memspace memspace(proxy, meta);
            Fixture::formatFixture(memspace, *meta);
        }
        auto fixture = db0::make_swine<Fixture>(*this, m_shared_object_list, proxy, meta);
        m_fixtures[fixture->getUUID()] = fixture;
        m_uuids[prefix_name] = fixture->getUUID();
        if (!m_current_fixture) {
            m_current_fixture = fixture;
        }
        return fixture;
    }

    db0::swine_ptr<Fixture> TestWorkspace::getFixture(std::uint64_t uuid, std::optional<AccessType>)
    {
        auto it = m_fixtures.find(uuid);
        if (it == m_fixtures.end()) {
            THROWF(db0::InputException) << "Fixture with UUID: " << uuid << " not found";
        }
        return it->second;
    }

    db0::swine_ptr<Fixture> TestWorkspace::getCurrentFixture()
    {
        if (!m_current_fixture) {
            THROWF(db0::InputException) << "No current fixture";
        }
        return m_current_fixture;
    }

    void TestWorkspace::tearDown()
    {
        TestWorkspaceBase::tearDown();
        close();
        m_shared_object_list.clear();
        m_slab_recycler.clear();
    }

    bool TestWorkspace::hasFixture(const PrefixName &prefix_name) const
    {
        auto it = m_uuids.find(prefix_name);
        if (it == m_uuids.end()) {
            return false;
        }
        return m_fixtures.find(it->second) != m_fixtures.end();
    }
    
    std::shared_ptr<LangCache> TestWorkspace::getLangCache() const {
        return m_lang_cache;
    }
    
}