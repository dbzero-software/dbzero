#include "LockedContext.hpp"

namespace db0

{

    std::shared_mutex LockedContext::m_locked_mutex;

    LockedContext::LockedContext(std::shared_ptr<Workspace> &workspace, std::shared_lock<std::shared_mutex> &&lock)\
        : m_workspace(workspace)
        , m_lock(std::move(lock))
    {
    }
    
    void LockedContext::close()
    {
    }
    
    std::shared_lock<std::shared_mutex> LockedContext::lockShared() {
        return std::shared_lock<std::shared_mutex>(m_locked_mutex);
    }

    std::unique_lock<std::shared_mutex> LockedContext::lockUnique() {
        return std::unique_lock<std::shared_mutex>(m_locked_mutex);
    }

    void LockedContext::makeNew(void *at, std::shared_ptr<Workspace> &workspace, std::shared_lock<std::shared_mutex> &&lock) {
        new (at) LockedContext(workspace, std::move(lock));
    }
    
    std::vector<std::pair<std::string, std::uint64_t> > LockedContext::getMutationLog() const {
        // FIXME: implement
        return {};
    }
    
}