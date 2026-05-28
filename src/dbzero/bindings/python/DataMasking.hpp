// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>

namespace db0

{

    enum class DataMaskingMode
    {
        RELEASE,
        DEBUG
    };

    struct DataMaskingState
    {
        PyObject *contextVar = nullptr;
        PyObject *missingValuePlaceholder = nullptr;
        bool hasMissingValuePlaceholder = false;
        DataMaskingMode mode = DataMaskingMode::RELEASE;

        DataMaskingState(PyObject *contextVar, PyObject *missingValuePlaceholder,
            bool hasMissingValuePlaceholder, DataMaskingMode mode)
            : contextVar(contextVar)
            , missingValuePlaceholder(missingValuePlaceholder)
            , hasMissingValuePlaceholder(hasMissingValuePlaceholder)
            , mode(mode)
        {
            Py_INCREF(contextVar);
            if (missingValuePlaceholder) {
                Py_INCREF(missingValuePlaceholder);
            }
        }

        bool matches(PyObject *otherContextVar, PyObject *otherMissingValuePlaceholder,
            bool otherHasMissingValuePlaceholder, DataMaskingMode otherMode) const
        {
            return contextVar == otherContextVar
                && missingValuePlaceholder == otherMissingValuePlaceholder
                && hasMissingValuePlaceholder == otherHasMissingValuePlaceholder
                && mode == otherMode;
        }
    };

    struct DataFilterState
    {
        PyObject *contextVar = nullptr;
        DataMaskingMode mode = DataMaskingMode::RELEASE;

        DataFilterState(PyObject *contextVar, DataMaskingMode mode)
            : contextVar(contextVar)
            , mode(mode)
        {
            Py_INCREF(contextVar);
        }

        bool matches(PyObject *otherContextVar, DataMaskingMode otherMode) const
        {
            return contextVar == otherContextVar
                && mode == otherMode;
        }
    };

}
