// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyLocks.hpp"
#include <dbzero/workspace/AtomicContext.hpp>

namespace db0::python

{

    GIL_Lock::GIL_Lock()
        : m_state(PyGILState_Ensure())
    {
    }

    GIL_Lock::~GIL_Lock() {
        PyGILState_Release(m_state);
    }
    
    WithGIL_Unlocked::WithGIL_Unlocked()
        : __thread_state(PyEval_SaveThread())
    {
    }

    WithGIL_Unlocked::~WithGIL_Unlocked() {
        PyEval_RestoreThread(__thread_state);
    }

    AtomicMutationApiScope::AtomicMutationApiScope(bool register_atomic_owner)
    {
        auto relation = db0::AtomicContext::getOwnerRelation();
        if (relation == db0::AtomicContext::OwnerRelation::same_thread_non_owner) {
            PyErr_SetString(
                PyExc_RuntimeError,
                "db0.async_atomic is active in another asyncio task; use db0.async_atomic() to serialize dbzero mutations"
            );
            m_ok = false;
            return;
        }

        db0::AtomicContext::waitIfBlockedByOwnerRelation(relation, false);
        if (register_atomic_owner && relation == db0::AtomicContext::OwnerRelation::owner) {
            db0::AtomicContext::enterMutatingApiAtomicOwner();
            m_atomic_owner = true;
        }
    }

    AtomicMutationApiScope::~AtomicMutationApiScope()
    {
        if (m_atomic_owner) {
            db0::AtomicContext::exitMutatingApiAtomicOwner();
        }
    }

} 
