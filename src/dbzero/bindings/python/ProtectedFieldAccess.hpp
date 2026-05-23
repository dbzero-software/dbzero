// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>

#include <dbzero/core/memory/swine_ptr.hpp>
#include <dbzero/object_model/class/FieldMask.hpp>
#include <dbzero/object_model/class/MemberID.hpp>

namespace db0
{
    class Fixture;
}

namespace db0::object_model
{
    class Class;
}

namespace db0::python
{
    bool checkProtectedFieldAccess(
        const db0::object_model::Class &type, const db0::swine_ptr<db0::Fixture> &fixture,
        db0::object_model::FieldMaskOptions accessOption,
        const db0::object_model::MemberLoc &memberLoc, const char *fieldName = nullptr
    );

    PyObject *checkProtectedFieldReadAccess(
        const db0::object_model::Class &type, const db0::swine_ptr<db0::Fixture> &fixture,
        const db0::object_model::MemberLoc &memberLoc, const char *fieldName = nullptr
    );

    bool checkProtectedFieldMutateAccess(
        const db0::object_model::Class &type, const db0::swine_ptr<db0::Fixture> &fixture,
        db0::object_model::FieldMaskOptions accessOption, const char *fieldName
    );
}
