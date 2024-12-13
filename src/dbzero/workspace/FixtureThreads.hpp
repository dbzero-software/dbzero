#pragma once

#include <thread>
#include <mutex>
#include <vector>
#include <memory>
#include <dbzero/core/memory/swine_ptr.hpp>
#include <functional>
#include <chrono>
#include <condition_variable>

namespace db0

{   

    class Fixture;
    
    /**
     * A thread object to poll fixture modification status
    */
    class FixtureThread
    {
    public:
        FixtureThread(std::function<void(Fixture &, std::uint64_t &status)> fx_function, std::uint64_t interval_ms,
            std::function<std::shared_ptr<void>()> ctx_function = {});
        virtual ~FixtureThread() = default;

        void addFixture(swine_ptr<Fixture> &fixture);

        void run();

        void stop();        
        
        void setInterval(std::uint64_t interval_ms);

    protected:
        std::function<void(Fixture &, std::uint64_t &)> m_fx_function;
        // optional context function
        std::function<std::shared_ptr<void>()> m_ctx_function;
        std::atomic<std::uint64_t> m_interval_ms;
        std::condition_variable m_cv;
        std::mutex m_mutex;
        bool m_stopped = false;
        // fixtures being monitored (weak pointer + last updated timestamp)
        std::vector<std::pair<weak_swine_ptr<Fixture>, std::uint64_t> > m_fixtures;        
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
        void tryRefresh(Fixture &fixture, std::uint64_t &status) const;
    };

    /**
     * The purpose of the AutoCommitThread is to commit changes from all read/write fixtures
     * after 250ms (unless configured differently) since the last modification
    */
    class AutoCommitThread: public FixtureThread
    {
    public:
        AutoCommitThread(std::uint64_t commit_interval_ms = 250);
        
    private:        
        void tryCommit(Fixture &fixture, std::uint64_t &status) const;
        std::shared_ptr<void> lockAtomicContext() const;
    };

} 
