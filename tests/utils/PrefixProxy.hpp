// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/memory/Prefix.hpp>
#include <optional>

namespace db0::tests

{

    /**
     * A Proxy interface wrapper for better observability
    */
    class PrefixProxy: public Prefix
    {
    public:
        PrefixProxy(std::shared_ptr<Prefix> prefix)
            : Prefix(prefix->getName())
            , m_prefix(prefix)
        {
        }

        MemLock mapRange(std::uint64_t address, std::size_t size, FlagSet<AccessOptions> options = {}) override
        {
            if (m_map_range_callback) {
                m_map_range_callback(address, size, options);
            }
            return m_prefix->mapRange(address, size, options);
        }

        StateNumType getStateNum(bool finalized) const override {
            return m_prefix->getStateNum(finalized);
        }
        
        std::size_t getPageSize() const override {
            return m_prefix->getPageSize();
        }
                
        std::uint64_t commit(ProcessTimer *timer = nullptr) override {
            return m_prefix->commit(timer);
        }

        void close(ProcessTimer * = nullptr) override {
            m_prefix->close();
        }
        
        std::uint64_t getLastUpdated() const override {
            return m_prefix->getLastUpdated();
        }

        std::uint64_t refresh() override {
            return m_prefix->refresh();
        }

        AccessType getAccessType() const override {
            return m_prefix->getAccessType();
        }

        std::shared_ptr<Prefix> getSnapshot(std::optional<StateNumType> state_num = {}) const override {
            return m_prefix->getSnapshot(state_num);
        }

        void setMapRangeCallback(std::function<void(std::uint64_t, std::size_t, FlagSet<AccessOptions>)> callback) {
            m_map_range_callback = callback;
        }

        void tearDown() {
            m_map_range_callback = nullptr;
        }

        BaseStorage &getStorage() const override {
            return m_prefix->getStorage();
        }
        
        std::size_t getDirtySize() const override {
            return m_prefix->getDirtySize();
        }

        std::size_t flushDirty(std::size_t limit) override {
            return m_prefix->flushDirty(limit);
        }
        
    private:        
        std::shared_ptr<Prefix> m_prefix;
        std::function<void(std::uint64_t, std::size_t, FlagSet<AccessOptions>)> m_map_range_callback;
    };

}