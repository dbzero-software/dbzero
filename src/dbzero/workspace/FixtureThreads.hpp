#pragma once

#include <dbzero/core/memory/swine_ptr.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <thread>
#include <mutex>
#include <vector>
#include <memory>
#include <functional>
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
        FixtureThread(std::function<void(Fixture &, std::uint64_t &status)> fx_function, std::uint64_t interval_ms,
            std::function<std::shared_ptr<FixtureThreadContextBase>()> ctx_function = {});
        virtual ~FixtureThread() = default;

        void addFixture(swine_ptr<Fixture> &fixture);

        void run();

        void stop();        
        
        void setInterval(std::uint64_t interval_ms);

    protected:
        std::function<void(Fixture &, std::uint64_t &)> m_fx_function;
        // optional context function
        std::function<std::shared_ptr<FixtureThreadContextBase>()> m_ctx_function;
        std::atomic<std::uint64_t> m_interval_ms;
        std::condition_variable m_cv;
        std::mutex m_mutex;
        bool m_stopped = false;

        struct FixtureHolder {
            weak_swine_ptr<Fixture> fixture;
            std::uint64_t last_updated;
        };
        // fixtures being monitored (weak pointer + last updated timestamp)
        std::vector<std::unique_ptr<FixtureHolder>> m_fixtures;   
    };

    /**
     * A thread object to poll fixture modification status
     * applicatble to read-only fixtures
    */
    class RefreshThread: public FixtureThread
    {
    public:
        RefreshThread();

    private:
        void tryRefresh(Fixture &fixture, std::uint64_t &status);

        using ClockType = std::chrono::high_resolution_clock;
        std::unordered_map<std::string, ClockType::time_point> m_last_updates;
    };

    /**
     * The purpose of the AutoCommitThread is to commit changes from all read/write fixtures
     * after 250ms (unless configured differently) since the last modification
    */
    class AutoCommitThread: public FixtureThread
    {
    public:
        AutoCommitThread(std::uint64_t commit_interval_ms = 250);
        
        // This lock prevents auto-commit thread collision (even if auto-commit thread is already waiting)
        static std::unique_lock<std::mutex> preventAutoCommit();

    private:
        using StateReachedCallbackList = Fixture::StateReachedCallbackList;

        /**
         * Acquires locks for safe execution and handles post-commit callbacks
         */
        class AutoCommitContext : public FixtureThreadContextBase
        {
            std::unique_lock<std::mutex> m_commit_lock;
            std::unique_lock<std::shared_mutex> m_locked_context_lock;
            std::unique_lock<std::mutex> m_atomic_lock;
            StateReachedCallbackList m_callbacks;

        public:
            AutoCommitContext(std::unique_lock<std::mutex> &&commit_lock, std::unique_lock<std::shared_mutex> &&locked_context_lock, 
                std::unique_lock<std::mutex> &&atomic_lock);
            
            virtual void finalize();

            void appendCallbacks(StateReachedCallbackList &&callbacks);
        };
        
        static std::mutex m_commit_mutex;
        std::weak_ptr<AutoCommitContext> m_tmp_context;
        
        void tryCommit(Fixture &fixture, std::uint64_t &status) const;
        std::shared_ptr<FixtureThreadContextBase> prepareContext();
    };
    
} 
