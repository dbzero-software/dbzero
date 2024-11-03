#pragma once

#include <thread>

namespace db0

{

    // A simple tool for debugging treading issues
    class ThreadTracker
    {
    public:
        static std::thread::id m_tracked_id;
        // begin tracking a specific thread (for which we expect unique access)
        static void beginUnique();
        // check if the current thread is a tracked unique one
        static void checkUnique();
        static void end();
    };

}
