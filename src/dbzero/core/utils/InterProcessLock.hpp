#pragma once
#include <string>
#include <Python.h>
#include <src/dbzero/workspace/LockFlags.hpp>


namespace db0 

{
    class InterProcessLock
    {
    public:
        InterProcessLock(const std::string & lockPath, LockFlags lock_flags);
        ~InterProcessLock();

        bool is_locked() const;

        // Assure that the lock is acquired. 
        // This can be occured when lock file is removed by another process
        void assure_lock();

    private:
        PyObject* m_lock = nullptr;
        LockFlags m_lock_flags;
        const std::string & m_lockPath;
    };
}