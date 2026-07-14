// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once
#include <string>
#include <Python.h>
#include <src/dbzero/workspace/LockFlags.hpp>


namespace db0 

{

    class InterProcessLock
    {
    public:
        using ObjectSharedPtr = db0::python::PyTypes::ObjectSharedPtr;

        InterProcessLock(const std::string &lockPath, LockFlags lock_flags);
        ~InterProcessLock();

        bool isLocked() const;
        
        // Assure that the lock is acquired. 
        // This can be occured when lock file is removed by another process
        void assureLocked();

    private:
        ObjectSharedPtr m_lock = nullptr;
        LockFlags m_lock_flags;
        const std::string m_lockPath;
    };

}