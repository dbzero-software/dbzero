#pragma once

#include <memory>
#include <dbzero/core/memory/Memspace.hpp>
#include <dbzero/core/memory/CacheRecycler.hpp>
#include "PrefixProxy.hpp"

namespace db0

{
    
    class TestWorkspace
    {
    public:
        TestWorkspace(std::size_t page_size = 4096, std::size_t cache_size = 2u << 30);
        ~TestWorkspace();
        
        /**
         * Opens a prefix associated memspace
        */
        Memspace getMemspace(const std::string &prefix);
        
        CacheRecycler &getCacheRecycler() {
            return m_cache_recycler;
        }

        void setMapRangeCallback(std::function<void(std::uint64_t, std::size_t, FlagSet<AccessOptions>)>);
        
        void tearDown();
    
    private:
        const std::size_t m_page_size;
        CacheRecycler m_cache_recycler;        
        std::shared_ptr<db0::tests::PrefixProxy> m_prefix;
    };
    
}