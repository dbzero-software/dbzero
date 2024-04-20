#pragma once

#include <memory>
#include <dbzero/core/memory/Memspace.hpp>
#include <dbzero/core/memory/CacheRecycler.hpp>
#include <dbzero/workspace/Snapshot.hpp>
#include "PrefixProxy.hpp"

namespace db0

{
    
    class TestWorkspaceBase
    {
    public:
        TestWorkspaceBase(std::size_t page_size = 4096, std::size_t cache_size = 2u << 30);
        ~TestWorkspaceBase();
        
        /**
         * Opens a prefix associated memspace
        */
        Memspace getMemspace(const std::string &prefix);
        
        CacheRecycler &getCacheRecycler() {
            return m_cache_recycler;
        }

        void setMapRangeCallback(std::function<void(std::uint64_t, std::size_t, FlagSet<AccessOptions>)>);
        
        void tearDown();
        
    protected:
        static constexpr auto TEST_MEMSPACE_UUID = 0x12345678;
        const std::size_t m_page_size;
        CacheRecycler m_cache_recycler;        
        std::shared_ptr<db0::tests::PrefixProxy> m_prefix;
    };
    
    class TestWorkspace: public TestWorkspaceBase, public db0::Snapshot
    {
    public:
        TestWorkspace(std::size_t page_size = 4096, std::size_t cache_size = 2u << 30);

        virtual db0::swine_ptr<Fixture> getFixture(
            const std::string &prefix_name, std::optional<AccessType> = AccessType::READ_WRITE) override;
        
        virtual db0::swine_ptr<Fixture> getFixture(std::uint64_t uuid, std::optional<AccessType> = {}) override;
        
        virtual db0::swine_ptr<Fixture> getCurrentFixture(std::optional<AccessType> = AccessType::READ_ONLY) override;
        
        bool close(const std::string &prefix_name) override;
        
        void close() override;    
    };

}