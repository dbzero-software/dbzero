// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/bindings/TypeId.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/core/serialization/Serializable.hpp>

namespace db0::python

{

    using TypeId = db0::bindings::TypeId;
    
    db0::swine_ptr<Fixture> getFixtureOf(PyObject*);
    PyObject *tryGetUUID(PyObject *);

}