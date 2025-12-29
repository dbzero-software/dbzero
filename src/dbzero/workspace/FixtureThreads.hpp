// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/memory/swine_ptr.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <thread>
#include <mutex>
#include <vector>
#include <memory>
#include <chrono>
#include <condition_variable>

namespace db0

{   
    class FixtureThreadContextBase
    {
    public:
        virtual ~FixtureThreadContextBase() = default;

        virtual void finalize() = 0;
    };

    /**
     * A thread object to poll fixture modification status
    */
    class FixtureThread
    {
    public:
        FixtureThread(std::uint64_t interval_ms);
        virtual ~FixtureThread() = default;

        void addFixture(swine_ptr<Fixture> &fixture);

        void run();

        void stop();        
        
        void setInterval(std::uint64_t interval_ms);

        virtual void onUpdate(Fixture &) = 0;

        virtual void onFixtureAdded(Fixture &);

    protected:
        std::atomic<std::uint64_t> m_interval_ms;
        std::condition_variable m_cv;
        std::mutex m_mutex;
        bool m_stopped = false;

        std::vector<weak_swine_ptr<Fixture>> m_fixtures;

        virtual void prepareContext() = 0;
        virtual void closeContext() = 0;
    };
    
    /**
     * A thread object to poll fixture modification status
     * applicatble to read-only fixtures
    */
    class FixtureThreadCallbacksContext;
    class RefreshThread: public FixtureThread
    {
    public:
        RefreshThread();

        virtual void onUpdate(Fixture &) override;

        virtual void onFixtureAdded(Fixture &) override;
        
    private:
        void tryRefresh(Fixture &fixture);

        using ClockType = std::chrono::high_resolution_clock;

        struct FixtureUpdateStatus {
            std::uint64_t last_updated;
            ClockType::time_point last_updated_check_tp;
        };

        std::unordered_map<std::uint64_t, FixtureUpdateStatus> m_fixture_status;
        std::shared_ptr<FixtureThreadCallbacksContext> m_context;

        void prepareContext() override;
        void closeContext() override;
    };

    /**
     * The purpose of the AutoCommitThread is to commit changes from all read/write fixtures
     * after 250ms (unless configured differently) since the last modification
    */
    class AutoSaveContext;
    class AutoCommitThread: public FixtureThread
    {
    public:
        AutoCommitThread(std::uint64_t commit_interval_ms = 250);
        
        virtual void onUpdate(Fixture &) override;
                
        // This lock prevents auto-commit thread collision (even if auto-commit thread is already waiting)
        static std::unique_lock<std::mutex> preventAutoCommit();

    private:
        static std::mutex m_commit_mutex;
        std::shared_ptr<AutoSaveContext> m_context;

        void prepareContext() override;
        void closeContext() override;
    };
    
} 
