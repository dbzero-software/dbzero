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

    PyObject* getPyHashAsPyObject(PyObject *);

    // calculate hash or raise an exception (unhashable type)
    std::int64_t getPyHash(PyObject *);

    template <TypeId type_id> std::int64_t getPyHashImpl(PyObject *);

    std::int64_t getPyHashDefaultImpl(PyObject *);

    // NOTE: in rare cases type may be hashable but hash cannot be calculate if instance does not exist
    // e.g. EnumValueRepr without actual EnumValue materialized yet
    // in such cases this function will not raise any exception but return std::nullopt
    template <typename CollectionT>
    std::optional<std::pair<std::int64_t, ObjectSharedPtr> > getPyHashIfExists(PyObject *obj_ptr, const CollectionT &collection)
    {
        // if not EnumValueRepr then simply calcualate
        if (!PyEnumValueRepr_Check(obj_ptr)) {
            return std::make_pair(getPyHash(obj_ptr), ObjectSharedPtr(obj_ptr));
        }
        
        // for EnumValueRepr we need to check if converstion to actual enum is possible
        auto fixture = collection.getFixture();
        auto &enum_factory = fixture->template get<db0::object_model::EnumFactory>();
        auto lang_enum = enum_factory.tryGetEnumLangValue(reinterpret_cast<PyEnumValueRepr*>(obj_ptr)->ext());
        if (!lang_enum) {
            // corresponding EnumValue does not exist
            return std::nullopt;
        }
        return std::make_pair(getPyHash(*lang_enum), lang_enum);
    }
    
}