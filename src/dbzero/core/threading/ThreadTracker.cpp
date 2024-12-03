#include "ThreadTracker.hpp"
#include <cassert>
#include <stdexcept>

namespace db0

{

    std::thread::id ThreadTracker::m_tracked_id;

    void ThreadTracker::beginUnique() {
        m_tracked_id = std::this_thread::get_id();
    }
    
    void ThreadTracker::checkUnique()
    {
        assert(m_tracked_id == std::thread::id() || m_tracked_id == std::this_thread::get_id());
        if (m_tracked_id != std::thread::id() && m_tracked_id != std::this_thread::get_id()) {
            throw std::runtime_error("ThreadTracker: unexpected thread access");
        }                    
    }

    void ThreadTracker::end() {
        m_tracked_id = std::thread::id();
    }

}