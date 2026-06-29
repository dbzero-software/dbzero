// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>
#include <cstddef>
#include "PyTypes.hpp"

namespace db0::python

{

    extern PyTypeObject PyRestrictedMethodType;

    class ScopedRestrictedMemoInit
    {
    public:
        ScopedRestrictedMemoInit();
        ~ScopedRestrictedMemoInit();

        ScopedRestrictedMemoInit(const ScopedRestrictedMemoInit &) = delete;
        ScopedRestrictedMemoInit &operator=(const ScopedRestrictedMemoInit &) = delete;

    private:
        bool m_was_enabled;
    };

    class ScopedRestrictedMemoUserCode
    {
    public:
        ScopedRestrictedMemoUserCode();
        ~ScopedRestrictedMemoUserCode();

        ScopedRestrictedMemoUserCode(const ScopedRestrictedMemoUserCode &) = delete;
        ScopedRestrictedMemoUserCode &operator=(const ScopedRestrictedMemoUserCode &) = delete;

    private:
        std::size_t m_previous_depth;
    };

    bool isRestrictedName(const char *attr_name);
    bool isRestrictedMemoContextActive();
    PyObject *tryRestrictedMemoGetattro(
        PyObject *memo_obj,
        PyObject *attr,
        const char *attr_name,
        PyTypes::ObjectSharedPtr &member
    );

}
