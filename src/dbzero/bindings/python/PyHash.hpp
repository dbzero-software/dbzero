// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once
#include <Python.h>
#include <dbzero/bindings/TypeId.hpp>
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/bindings/python/PyTypes.hpp>
#include <dbzero/bindings/python/types/PyEnum.hpp>
#include <dbzero/object_model/enum/EnumValue.hpp>
#include <dbzero/object_model/enum/EnumFactory.hpp>
#include <dbzero/workspace/Fixture.hpp>

namespace db0::python

{   

    using TypeId = db0::bindings::TypeId;
    using ObjectSharedPtr = PyTypes::ObjectSharedPtr;

    PyObject* getPyHashAsPyObject(db0::swine_ptr<Fixture> &, PyObject *);

    // calculate hash or raise an exception (unhashable type)
    std::int64_t getPyHash(db0::swine_ptr<Fixture> &, PyObject *);
    
    template <TypeId type_id> std::int64_t getPyHashImpl(db0::swine_ptr<Fixture> &, PyObject *);
    
    // NOTE: in rare cases type may be hashable but hash cannot be calculate if instance does not exist
    // e.g. EnumValueRepr without actual EnumValue materialized yet
    // in such cases this function will not raise any exception but return std::nullopt    
    std::optional<std::pair<std::int64_t, ObjectSharedPtr> > getPyHashIfExists(
        db0::swine_ptr<Fixture> &fixture, PyObject *obj_ptr);
    
}