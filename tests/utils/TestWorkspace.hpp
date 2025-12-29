// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <memory>
#include <mutex>
#include <dbzero/core/memory/Memspace.hpp>
#include <dbzero/core/memory/CacheRecycler.hpp>
#include <dbzero/core/memory/VObjectCache.hpp>
#include <dbzero/core/memory/SlabItem.hpp>
#include <dbzero/core/memory/Recycler.hpp>
#include <dbzero/object_model/LangCache.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Snapshot.hpp>
#include <dbzero/workspace/PrefixName.hpp>
#include "PrefixProxy.hpp"
#include "EmbeddedAllocator.hpp"

namespace db0

{
    
    class TestWorkspaceBase
    {
    public:
        using AllocCallbackT = EmbeddedAllocator::AllocCallbackT;
        TestWorkspaceBase(std::size_t page_size = 4096, std::size_t cache_size = 2u << 30);
        ~TestWorkspaceBase();
        
        /**
         * Opens a prefix associated memspace
        */
        Memspace getMemspace(const PrefixName &, AllocCallbackT = {});
        
        CacheRecycler &getCacheRecycler() {
            return m_cache_recycler;
        }

        void setMapRangeCallback(std::function<void(std::uint64_t, std::size_t, FlagSet<AccessOptions>)>);
        
        void tearDown();
        
    protected:
        static constexpr auto TEST_MEMSPACE_UUID = 0x12345678;
        const std::size_t m_page_size;
        std::atomic<std::size_t> m_dirty_meter = 0;
        CacheRecycler m_cache_recycler;
        std::shared_ptr<db0::tests::PrefixProxy> m_prefix;
    };
    
    class TestWorkspace: public TestWorkspaceBase, public db0::Snapshot
    {
    public:
        TestWorkspace(std::size_t page_size = 4096, std::size_t slab_size = 1u << 20,
            std::size_t cache_size = 2u << 30);

        bool hasFixture(const PrefixName &) const override;
        
        db0::swine_ptr<Fixture> tryGetFixture(const PrefixName &prefix_name, 
            std::optional<AccessType> = {}) override;
        
        db0::swine_ptr<Fixture> tryGetFixture(std::uint64_t uuid, std::optional<AccessType> = {}) override;
        
        db0::swine_ptr<Fixture> tryFindFixture(const PrefixName &) const override;

        db0::swine_ptr<Fixture> getCurrentFixture() override;
        
        bool close(const PrefixName &) override;
        
        void close(bool as_defunct = false, ProcessTimer * = nullptr) override;

        std::shared_ptr<LangCache> getLangCache() const override;

        bool isMutable() const override;

        void tearDown();

        std::size_t size() const override;
        
    private:
        const std::size_t m_slab_size;
        FixedObjectList m_shared_object_list;
        Recycler<db0::SlabItem> m_slab_recycler;
        db0::swine_ptr<Fixture> m_current_fixture;
        std::unordered_map<std::uint64_t, db0::swine_ptr<Fixture> > m_fixtures;
        std::unordered_map<std::string, std::uint64_t> m_uuids;
        mutable std::shared_ptr<db0::LangCache> m_lang_cache;
    };
    
}