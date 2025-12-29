// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyReflectionAPI.hpp"
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/workspace/PrefixName.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/value/ObjectId.hpp>
#include "PyToolkit.hpp"
#include <dbzero/bindings/python/types/PyClass.hpp>
#include <dbzero/bindings/python/PySafeAPI.hpp>

namespace db0::python

{
    
    PyObject *tryGetPrefixes()
    {        
        auto &fixture_catalog = PyToolkit::getPyWorkspace().getWorkspace().getFixtureCatalog();
        fixture_catalog.refresh();
        auto data = fixture_catalog.getData();
        // return as python list of tuples
        auto py_list = Py_OWN(PyList_New(0));
        // prefix name / UUID pairs
        for (auto [name, uuid]: data) {
            auto py_tuple = Py_OWN(PySafeTuple_Pack(Py_OWN(PyUnicode_FromString(name.c_str())), 
                Py_OWN(PyLong_FromUnsignedLongLong(uuid)))
            );
            PySafeList_Append(*py_list, py_tuple);
        }
        return py_list.steal();
    }
    
    PyObject *getSingletonUUID(const db0::object_model::Class &type)
    {
        if (!type.isSingleton() || !type.isExistingSingleton()) {
            Py_RETURN_NONE;
        }
        return PyUnicode_FromString(type.getSingletonObjectId().toUUIDString().c_str());
    }
    
    PyObject *getMemoClasses(db0::swine_ptr<Fixture> fixture)
    {
        assert(fixture);
        auto &class_factory = fixture->get<db0::object_model::ClassFactory>();
        // collect class info as tuples
        auto py_list = Py_OWN(PyList_New(0));
        class_factory.forAll([&](const db0::object_model::Class &type) {
            PySafeList_Append(*py_list, Py_OWN(tryGetTypeInfo(type)));
        });
        return py_list.steal();
    }
    
    PyObject *tryGetMemoClasses(const char *prefix_name, std::uint64_t prefix_uuid)
    {   
        auto &workspace = PyToolkit::getPyWorkspace().getWorkspace();
        if (!prefix_name && !prefix_uuid) {
            // retrieve from the default (current) prefix
            return getMemoClasses(workspace.getCurrentFixture());
        }

        if (!prefix_name && !prefix_uuid) {
            THROWF(db0::InputException) << "Invalid arguments (both prefix name and UUID are empty)";
        }
        db0::swine_ptr<Fixture> fixture;
        
        if (prefix_name) {
            fixture = workspace.getFixture(prefix_name, AccessType::READ_ONLY);
        }
        if (prefix_uuid) {
            if (fixture) {
                // validate that the prefix name & UUID match
                if (fixture->getUUID() != prefix_uuid) {
                    THROWF(db0::InputException) << "Prefix name and UUID mismatch";
                }
            } else {
                fixture = workspace.getFixture(prefix_uuid, AccessType::READ_ONLY);
            }
        }
        return getMemoClasses(fixture);    
    }
        
}