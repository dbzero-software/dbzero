// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "LockedContext.hpp"
#include <dbzero/workspace/Workspace.hpp>

namespace db0

{

    std::shared_mutex LockedContext::m_locked_mutex;
    
    LockedContext::LockedContext(std::shared_ptr<Workspace> &workspace, std::shared_lock<std::shared_mutex> &&lock)
        : m_workspace(workspace)
        , m_locked_section_id(m_workspace->beginLocked())
        , m_lock(std::move(lock))
    {
    }
    
    void LockedContext::close()
    {
        auto callback = [&](const std::string &prefix_name, std::uint64_t state_num) {
            m_mutation_log.emplace_back(prefix_name, state_num);
        };
        m_workspace->endLocked(m_locked_section_id, callback);
        m_lock.unlock();
    }
    
    std::shared_lock<std::shared_mutex> LockedContext::lockShared() {
        return std::shared_lock<std::shared_mutex>(m_locked_mutex);
    }

    std::unique_lock<std::shared_mutex> LockedContext::lockUnique() {
        return std::unique_lock<std::shared_mutex>(m_locked_mutex);
    }
    
    std::vector<std::pair<std::string, std::uint64_t> > LockedContext::getMutationLog() const {
        return m_mutation_log;
    }
    
}