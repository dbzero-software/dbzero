#include "PyAPI_Lock.hpp"
#include "PyToolkit.hpp"

namespace db0::python

{

    PyAPI_Lock::PyAPI_Lock() {
        PyToolkit::lockApi();
    }

    PyAPI_Lock::~PyAPI_Lock() {
        PyToolkit::unlockApi();
    }

    void PyAPI_Lock::lock() {
        PyToolkit::lockApi();
    }
    
    void PyAPI_Lock::unlock() {
        PyToolkit::unlockApi();
    }
    
    PyAPI_Unlock::PyAPI_Unlock()
        : m_unlocked(PyToolkit::tryUnlockApi())
    {
    }

    PyAPI_Unlock::~PyAPI_Unlock() 
    {
        if (m_unlocked) {
            PyToolkit::lockApi();
        }
    }

}