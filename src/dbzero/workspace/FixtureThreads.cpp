#include "FixtureThreads.hpp"
#include <dbzero/core/memory/Prefix.hpp>
#include <dbzero/object_model/LangConfig.hpp>
#include <dbzero/core/threading/ThreadTracker.hpp>
#include "AtomicContext.hpp"
#include "LockedContext.hpp"

namespace db0

{

    FixtureThread::FixtureThread(std::function<void(Fixture &, std::uint64_t &)> fx_function, std::uint64_t interval_ms,
        std::function<std::shared_ptr<FixtureThreadContextBase>()> ctx_function)
        : m_fx_function(std::move(fx_function))
        , m_ctx_function(std::move(ctx_function))
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
            std::unique_lock<std::mutex> lock(m_mutex);
            m_cv.wait_for(lock, std::chrono::milliseconds(m_interval_ms));
            if (m_stopped) {
                break;
            }
            // prepare commit context if configured
            std::shared_ptr<FixtureThreadContextBase> context;
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
            for (FixtureHolder *holder_ptr : fixtures) {
                if (auto fixture = holder_ptr->fixture.lock()) {
                    m_fx_function(*fixture, holder_ptr->last_updated);
                }      
            }

            if (context) {
                context->finalize();
            }
        }
    }
    
    RefreshThread::RefreshThread()
        : FixtureThread([this](Fixture &fx, std::uint64_t &status) { tryRefresh(fx, status); }, 5)
    {
    }
    
    void RefreshThread::tryRefresh(Fixture &fixture, std::uint64_t &status)
    {
        auto prefix_ptr = fixture.getPrefixPtr();
        // prefix_ptr may not exist a fixture has already been closed
        if (!prefix_ptr) {
            return;
        }
        std::string prefix_name = prefix_ptr->getName();
        auto now = ClockType::now();

        auto last_updated = prefix_ptr->getLastUpdated();
        if (last_updated != status) {
            fixture.onUpdated();
            status = last_updated;
            m_last_updates[prefix_name] = now;
        }
        else
        {
            auto it = m_last_updates.find(prefix_name);
            if(it == m_last_updates.end()) {
                m_last_updates[prefix_name] = now;
                return;
            }

            auto fixture_last_update_time = it->second;
            if((now - fixture_last_update_time) > std::chrono::seconds(5))
            {
                // This is to protect against edge-case hang on 'wait' function,
                // caused by refresh thread not picking up all cases when prefix file is modified.
                // The refresh mechanism can potentially be improved in the future.
                fixture.onUpdated();
                m_last_updates[prefix_name] = now;
            }
        }
    }
    
    std::mutex AutoCommitThread::m_commit_mutex;

    AutoCommitThread::AutoCommitThread(std::uint64_t commit_interval_ms)
        : FixtureThread(
            [this](Fixture &fx, std::uint64_t &status) { tryCommit(fx, status); },
            commit_interval_ms,
            // NOTE: context function acquires locks to make commit / (atomic | locked context) operations mutually exclusive
            [this]() { return prepareContext(); }
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
        // NOTE: since this a separate thread, we must acuire the language interpreter's lock (where required)
        auto lang_lock = LangToolkit::ensureLocked();
#ifndef NDEBUG
        ThreadTracker::beginUnique();
#endif        
        auto callbacks = fixture.onAutoCommit();
        // This should always succeed. We just want the context lifetime to be managed by the FixtureThread.
        auto context = m_tmp_context.lock();
        assert(context);
        // These callbacks have to be executed when 'everything' is unlocked. Otherwise we are risking a deadlock.
        context->appendCallbacks(std::move(callbacks));
#ifndef NDEBUG
        ThreadTracker::end();
#endif        
    }

    std::shared_ptr<FixtureThreadContextBase> AutoCommitThread::prepareContext()
    {
        assert(!m_tmp_context.lock() && "Only one AutoCommitContext should exist at the time!");
        auto commit_lock = std::unique_lock<std::mutex>(m_commit_mutex);
        // must acquire unique lock-context's lock
        auto locked_context_lock = db0::LockedContext::lockUnique();
        // and the atomic lock next (order is relevant here !!)
        auto atomic_lock = db0::AtomicContext::lock();
        auto context = std::make_shared<AutoCommitContext>(std::move(commit_lock),
            std::move(locked_context_lock), std::move(atomic_lock)
        );
        // To collect callbacks from fixtures as we proceed with commiting
        m_tmp_context = context;
        return context;
    }

    AutoCommitThread::AutoCommitContext::AutoCommitContext(
        std::unique_lock<std::mutex> &&commit_lock,
        std::unique_lock<std::shared_mutex> &&locked_context_lock,
        std::unique_lock<std::mutex> &&atomic_lock)
        : m_commit_lock(std::move(commit_lock))
        , m_locked_context_lock(std::move(locked_context_lock))
        , m_atomic_lock(std::move(atomic_lock))
    {}
    
    void AutoCommitThread::AutoCommitContext::finalize()
    {
        m_locked_context_lock.unlock();
        m_atomic_lock.unlock();
        m_commit_lock.unlock();
        for(auto &callback : m_callbacks) {
            callback->execute();
        }
    }
    
    void AutoCommitThread::AutoCommitContext::appendCallbacks(StateReachedCallbackList &&callbacks)
    {
        // As of writing this, the purpose of these callbacks solely is to notify observers of prefix state number being reached
        std::move(callbacks.begin(), callbacks.end(), std::back_inserter(m_callbacks));
    }

    std::unique_lock<std::mutex> AutoCommitThread::preventAutoCommit() {
        return std::unique_lock<std::mutex>(m_commit_mutex);
    }

}
