#include "FixtureThreads.hpp"
#include <dbzero/core/memory/Prefix.hpp>
#include <dbzero/object_model/LangConfig.hpp>
#include <dbzero/core/threading/ThreadTracker.hpp>
#include "AtomicContext.hpp"
#include "LockedContext.hpp"

namespace db0

{
    class FixtureThreadCallbacksContext : public FixtureThreadContextBase
    {
    public:
        using StateReachedCallbackList = Fixture::StateReachedCallbackList;

        virtual ~FixtureThreadCallbacksContext() = default;

        virtual void finalize() override
        {
            for(auto &callback : m_callbacks) {
                callback->execute();
            }
        }

        void appendCallbacks(StateReachedCallbackList &&callbacks)
        {
            // As of writing this, the purpose of these callbacks is solely to notify observers of prefix state number being reached
            std::move(callbacks.begin(), callbacks.end(), std::back_inserter(m_callbacks));
        }

    private:
        StateReachedCallbackList m_callbacks;
    };



    FixtureThread::FixtureThread(std::uint64_t interval_ms)
        : m_interval_ms(interval_ms)        
    {
    } 
    
    void FixtureThread::addFixture(swine_ptr<Fixture> &fixture)
    {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_fixtures.emplace_back(fixture);
        }
        onFixtureAdded(*fixture);
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
            lock.unlock();
            std::shared_ptr<FixtureThreadContextBase> context = prepareContext();
            // collect fixtures first
            std::vector<weak_swine_ptr<Fixture>> fixtures;
            fixtures.reserve(m_fixtures.size());
            lock.lock();
            for (auto it = m_fixtures.begin(); it != m_fixtures.end(); ) {
                auto fixture_ptr = it->lock();
                if (!fixture_ptr) {
                    it = m_fixtures.erase(it);
                    continue;
                }
                fixtures.push_back(fixture_ptr);                
                ++it;
            }
            // then process as unlocked
            lock.unlock();
            for (weak_swine_ptr<Fixture> &fixture_weak_ptr : fixtures) {
                if (auto fixture_ptr = fixture_weak_ptr.lock()) {
                    onUpdate(*fixture_ptr);
                }      
            }

            if (context) {
                context->finalize();
            }
        }
    }

    void FixtureThread::onFixtureAdded(Fixture &)
    {
    }

    std::shared_ptr<FixtureThreadContextBase> FixtureThread::prepareContext() {
        return nullptr;
    }



    RefreshThread::RefreshThread()
        : FixtureThread(250)
    {
    }

    void RefreshThread::onFixtureAdded(Fixture &fixture)
    {
        std::uint64_t uuid = fixture.getUUID();
        assert(m_fixture_status.find(uuid) == m_fixture_status.end());
        m_fixture_status[uuid] = FixtureUpdateStatus{fixture.getPrefix().getLastUpdated(), ClockType::now()};
    }

    std::shared_ptr<FixtureThreadContextBase> RefreshThread::prepareContext()
    {
        auto context = std::make_shared<FixtureThreadCallbacksContext>();
        m_tmp_context = context;
        return context;
    }
    
    void RefreshThread::onUpdate(Fixture &fixture)
    {
        auto prefix_ptr = fixture.getPrefixPtr();
        // prefix_ptr may not exist a fixture has already been closed
        if (!prefix_ptr) {
            return;
        }

        std::uint64_t uuid = fixture.getUUID();
        auto last_updated = prefix_ptr->getLastUpdated();
        auto now = ClockType::now();

        auto it = m_fixture_status.find(uuid);
        assert(it != m_fixture_status.end());        
        FixtureUpdateStatus &update_status = it->second;
        if (last_updated != update_status.last_updated) {
            tryRefresh(fixture);
            update_status.last_updated = last_updated;
            update_status.last_updated_check_tp = now;
        }
        else
        {
            if((now - update_status.last_updated_check_tp) > std::chrono::seconds(5))
            {
                // This is to protect against edge-case hang on 'wait' function,
                // caused by refresh thread not picking up all cases when prefix file is modified.
                // The refresh mechanism can potentially be improved in the future.
                tryRefresh(fixture);
                update_status.last_updated_check_tp = now;
            }
        }
    }

    void RefreshThread::tryRefresh(Fixture &fixture)
    {
        using LangToolkit = db0::object_model::LangConfig::LangToolkit;
        auto api_lock = LangToolkit::lockApi();
        auto lang_lock = LangToolkit::ensureLocked();

        auto callbacks = fixture.onRefresh();
        auto context = m_tmp_context.lock();
        assert(context);
        context->appendCallbacks(std::move(callbacks));
    }



    std::mutex AutoCommitThread::m_commit_mutex;

    /**
     * Acquires locks for safe execution and handles post-commit callbacks
     */
    class AutoCommitContext : public FixtureThreadCallbacksContext
    {
        std::unique_lock<std::mutex> m_commit_lock;
        std::unique_lock<std::shared_mutex> m_locked_context_lock;
        std::unique_lock<std::mutex> m_atomic_lock;

    public:
        AutoCommitContext(
            std::unique_lock<std::mutex> &&commit_lock,
            std::unique_lock<std::shared_mutex> &&locked_context_lock, 
            std::unique_lock<std::mutex> &&atomic_lock)
            : m_commit_lock(std::move(commit_lock))
            , m_locked_context_lock(std::move(locked_context_lock))
            , m_atomic_lock(std::move(atomic_lock))
        {}
        
        virtual void finalize() override
        {
            m_locked_context_lock.unlock();
            m_atomic_lock.unlock();
            m_commit_lock.unlock();
            FixtureThreadCallbacksContext::finalize();
        }
    };

    AutoCommitThread::AutoCommitThread(std::uint64_t commit_interval_ms)
        : FixtureThread(commit_interval_ms)
    {
    }
    
    void AutoCommitThread::onUpdate(Fixture &fixture)
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

    std::unique_lock<std::mutex> AutoCommitThread::preventAutoCommit() {
        return std::unique_lock<std::mutex>(m_commit_mutex);
    }

}
