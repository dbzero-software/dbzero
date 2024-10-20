#pragma once

#define PY_API_FUNC db0::python::PyAPI_Lock api_lock;
#define PY_API_UNLOCK bool py_api_unlocked = PyToolkit::tryUnlockApi();
#define PY_API_LOCK if (py_api_unlocked) PyToolkit::lockApi();
#define WITH_PY_API_UNLOCKED db0::python::PyAPI_Unlock api_unlock;

namespace db0::python

{

    // the scoped API lock utility
    struct PyAPI_Lock
    {
        PyAPI_Lock();
        ~PyAPI_Lock();
        
        void lock();
        void unlock();
    };

    // the scoped Python API unlock utility
    class PyAPI_Unlock
    {
    public:
        PyAPI_Unlock();
        ~PyAPI_Unlock();
    private:
        const bool m_unlocked;
    };
    
}