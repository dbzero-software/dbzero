#include "TestWorkspace.hpp"
#include "EmbeddedAllocator.hpp"

#include <dbzero/core/memory/PrefixImpl.hpp>
#include <dbzero/core/storage/Storage0.hpp>

namespace db0

{
    
    TestWorkspaceBase::TestWorkspaceBase(std::size_t page_size, std::size_t cache_size)
        : m_page_size(page_size)
        , m_cache_recycler(cache_size)
    {
    }
    
    TestWorkspaceBase::~TestWorkspaceBase()
    {
        if (m_prefix) {
            m_prefix->close();
        }
    }
    
    Memspace TestWorkspaceBase::getMemspace(const std::string &name)
    {
        using PrefixT = PrefixImpl<db0::Storage0>;
        if (!m_prefix) {
            auto prefix = std::shared_ptr<Prefix>(new PrefixT(name, m_cache_recycler, {}, m_page_size));
            m_prefix = std::make_shared<db0::tests::PrefixProxy>(prefix);
        }
        auto allocator = std::make_shared<EmbeddedAllocator>();
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

    bool TestWorkspace::close(const std::string &) {
        return false;
    }
        
    void TestWorkspace::close() 
    {
    }

}