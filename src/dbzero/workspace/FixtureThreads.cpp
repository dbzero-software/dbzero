#include "FixtureThreads.hpp"
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/core/memory/Prefix.hpp>
#include <dbzero/object_model/LangConfig.hpp>
#include <dbzero/core/threading/ThreadTracker.hpp>
#include "AtomicContext.hpp"
#include "LockedContext.hpp"

namespace db0

{

    FixtureThread::FixtureThread(std::function<void(Fixture &, std::uint64_t &)> fx_function, std::uint64_t interval_ms,
        std::function<std::shared_ptr<void>()> ctx_function)
        : m_fx_function(fx_function)
        , m_ctx_function(ctx_function)
        , m_interval_ms(interval_ms)        
    {
    } 
    
    void FixtureThread::addFixture(swine_ptr<Fixture> &fixture)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_fixtures.emplace_back(new FixtureHolder{fixture, fixture->getPrefix().getLastUpdated()});
    }

    void FixtureThread::setInterval(std::uint64_t interval_ms) {
        m_interval_ms = interval_ms;
    }

    void FixtureThread::stop()
    {
        {
            std::unique_lock<std::mutex> lock(m_mutex);
            m_stopped = true;
        }
        m_cv.notify_all();
    }
    
    void FixtureThread::run()
    {        
        while (true) {
            // std::this_thread::sleep_for(std::chrono::milliseconds(m_interval_ms));
            std::unique_lock<std::mutex> lock(m_mutex);
            m_cv.wait_for(lock, std::chrono::milliseconds(m_interval_ms));
            if (m_stopped) {
                break;
            }
            // prepare commit context if configured
            std::shared_ptr<void> context;
            if (m_ctx_function) {
                lock.unlock();
                context = m_ctx_function();
                lock.lock();
            }
            // collect fixtures first
            std::vector<FixtureHolder*> fixtures;
            fixtures.reserve(m_fixtures.size());
            for (auto it = m_fixtures.begin(); it != m_fixtures.end(); ) {
                auto &holder_ptr = *it;
                if (!holder_ptr->fixture.lock()) {
                    it = m_fixtures.erase(it);
                    continue;
                }
                fixtures.push_back(holder_ptr.get());                
                ++it;
            }

            // then process as unlocked
            lock.unlock();
            for(FixtureHolder *holder_ptr : fixtures) {
                if (auto fixture = holder_ptr->fixture.lock()) {
                    m_fx_function(*fixture, holder_ptr->last_updated);
                }      
            }         
        }
    }
    
    RefreshThread::RefreshThread()
        : FixtureThread([this](Fixture &fx, std::uint64_t &status) { tryRefresh(fx, status); }, 5)
    {
    }
    
    void RefreshThread::tryRefresh(Fixture &fixture, std::uint64_t &status) const
    {
        auto prefix_ptr = fixture.getPrefixPtr();
        // prefix_ptr may not exist a fixture has already been closed
        if (!prefix_ptr) {
            return;
        }

        auto last_updated = prefix_ptr->getLastUpdated();
        if (last_updated != status) {
            fixture.onUpdated();
            status = last_updated;
        }
    }
    
    AutoCommitThread::AutoCommitThread(std::uint64_t commit_interval_ms)
        : FixtureThread(
            [this](Fixture &fx, std::uint64_t &status) { tryCommit(fx, status); },
            commit_interval_ms,
            // NOTE: context function acquires locks to make commit / (atomic | locked context) operations mutually exclusive
            [this]() { return lockAutoCommit(); }
        )
    {
    }
    
    void AutoCommitThread::tryCommit(Fixture &fixture, std::uint64_t &) const
    {
        using LangToolkit = db0::object_model::LangConfig::LangToolkit;

        // need to lock the language API first
        // otherwise it may deadlock on trying to invoke API calls from auto-commit 
        // (e.g. instance destruction triggered by LangCache::clear)
        auto __api_lock = LangToolkit::lockApi();
#ifndef NDEBUG
        ThreadTracker::beginUnique();
#endif        
        fixture.onAutoCommit();
#ifndef NDEBUG
        ThreadTracker::end();
#endif        
    }
    
    // auto-commit lock object
    struct AC_Lock
    {
        std::unique_lock<std::shared_mutex> m_locked_context_lock;
        std::unique_lock<std::mutex> m_atomic_lock;
        AC_Lock(std::unique_lock<std::shared_mutex> &&locked_context_lock, std::unique_lock<std::mutex> &&atomic_lock)
            : m_locked_context_lock(std::move(locked_context_lock))
            , m_atomic_lock(std::move(atomic_lock))
        {
        }
    };
    
    std::shared_ptr<void> AutoCommitThread::lockAutoCommit() const
    {
        // must acquire unique lock-context's lock
        auto locked_context_lock = db0::LockedContext::lockUnique();
        // and the atomic lock next (order is relevant here !!)
        auto atomic_lock = db0::AtomicContext::lock();
        return db0::make_shared_void<AC_Lock>(std::move(locked_context_lock), std::move(atomic_lock));
    }

}