#include "FixtureThreads.hpp"
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/core/memory/Prefix.hpp>
#include <dbzero/object_model/LangConfig.hpp>
#include "AtomicContext.hpp"

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
        m_fixtures.push_back({fixture, fixture->getPrefix().getLastUpdated()});
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
                context = m_ctx_function();
            }
            for (auto it = m_fixtures.begin(); it != m_fixtures.end(); ) {
                auto fixture = it->first.lock();
                if (!fixture) {
                    it = m_fixtures.erase(it);
                    continue;
                }
                m_fx_function(*fixture, it->second);
                ++it;
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
            commit_interval_ms / 2,
            // NOTE: context function locks AtomicContext to make commit / atomic operations are mutually exclusive
            [this]() { return lockAtomicContext(); }
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
        fixture.onAutoCommit();
    }
    
    struct AC_Lock
    {
        std::unique_lock<std::mutex> m_lock;
        AC_Lock(std::unique_lock<std::mutex> &&lock)
            : m_lock(std::move(lock))
        {            
        }
    };

    std::shared_ptr<void> AutoCommitThread::lockAtomicContext() const {
        return db0::make_shared_void<AC_Lock>(db0::AtomicContext::lock());
    }

}