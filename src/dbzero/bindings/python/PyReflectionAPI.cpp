#include "PyReflectionAPI.hpp"
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/value/ObjectId.hpp>
#include "PyToolkit.hpp"

namespace db0::python

{
    
    PyObject *tryGetPrefixes()
    {        
        auto &fixture_catalog = PyToolkit::getPyWorkspace().getWorkspace().getFixtureCatalog();
        fixture_catalog.refresh();
        auto data = fixture_catalog.getData();
        // return as python list of tuples
        PyObject *py_list = PyList_New(0);
        // prefix name / UUID pairs
        for (auto [name, uuid]: data) {
            PyObject *py_tuple = PyTuple_Pack(2, PyUnicode_FromString(name.c_str()), PyLong_FromUnsignedLongLong(uuid));            
            PyList_Append(py_list, py_tuple);
        }
        return py_list;
    }
    
    PyObject *getSingletonUUID(const db0::object_model::Class &type)
    {
        if (!type.isSingleton() || !type.isExistingSingleton()) {
            return Py_None;
        }
        return PyUnicode_FromString(type.getSingletonObjectId().toUUIDString().c_str());
    }
    
    PyObject *getMemoClasses(db0::swine_ptr<Fixture> fixture)
    {
        assert(fixture);
        auto &class_factory = fixture->get<db0::object_model::ClassFactory>();
        // collect class info as tuples
        PyObject *py_list = PyList_New(0);
        class_factory.forAll([&](std::shared_ptr<const db0::object_model::Class> type) {
            // do not report MemoBase type
            if (type->isMemoBase()) {
                return;
            }
            
            PyObject *py_tuple = nullptr;
            if (type->isSingleton()) {
                // name, module, memo_uuid, is_singleton, singleton_uuid
                py_tuple = PyTuple_Pack(5,
                    PyUnicode_FromString(type->getTypeName().c_str()),
                    PyUnicode_FromString(type->getModuleName().c_str()),
                    PyUnicode_FromString(type->getClassId().toUUIDString().c_str()),
                    type->isSingleton() ? Py_True : Py_False,
                    getSingletonUUID(*type)
                );
            } else {
                // name, module, memo_uuid
                py_tuple = PyTuple_Pack(3,
                    PyUnicode_FromString(type->getTypeName().c_str()), 
                    PyUnicode_FromString(type->getModuleName().c_str()),
                    PyUnicode_FromString(type->getClassId().toUUIDString().c_str())
                );
            }
            PyList_Append(py_list, py_tuple);
        });
        return py_list;
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