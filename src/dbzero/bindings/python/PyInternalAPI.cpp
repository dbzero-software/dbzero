#include "PyInternalAPI.hpp"
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/object_model/class.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/workspace/WorkspaceView.hpp>
#include <dbzero/core/serialization/Types.hpp>

namespace db0::python

{

    ObjectId extractObjectId(PyObject *args)
    {
        // extact ObjectId from args
        PyObject *py_object_id;
        if (!PyArg_ParseTuple(args, "O", &py_object_id)) {
            THROWF(db0::InputException) << "Invalid argument type";
        }
        
        if (!ObjectId_Check(py_object_id)) {
            THROWF(db0::InputException) << "Invalid argument type";
        }
        
        return *reinterpret_cast<ObjectId*>(py_object_id);
    }

    PyObject *fetchObject(ObjectId object_id)
    {
        auto &workspace = PyToolkit::getPyWorkspace().getWorkspace();
        auto fixture = workspace.getFixture(object_id.m_fixture_uuid, AccessType::READ_ONLY);
        assert(fixture);
        fixture->refreshIfUpdated();
        // open from specific fixture
        return fetchObject(fixture, object_id);
    }
    
    PyObject *fetchObject(db0::swine_ptr<Fixture> &fixture, ObjectId object_id)
    {
        auto storage_class = object_id.m_typed_addr.getType();
        auto addr = object_id.m_typed_addr;
        auto &lang_cache = fixture->getLangCache();
        if (storage_class == db0::object_model::StorageClass::OBJECT_REF) {
            // try pulling from cache first
            auto object_ptr = lang_cache.get(addr);
            if (object_ptr) {
                Py_INCREF(object_ptr);
                return object_ptr;
            }

            auto &class_factory = fixture->get<db0::object_model::ClassFactory>();
            // unload with instance_id validation
            object_ptr = PyToolkit::unloadObject(fixture, addr, class_factory, object_id.m_instance_id);
            // keep in cache (weak reference only)
            fixture->getLangCache().add(addr, object_ptr, false);
            return object_ptr;
        }

        if (storage_class == db0::object_model::StorageClass::DB0_LIST) {
            // unload with instance_id validation
            return PyToolkit::unloadList(fixture, object_id.m_typed_addr, object_id.m_instance_id);
        } 
        
        if (storage_class == db0::object_model::StorageClass::DB0_INDEX) {
            // unload with instance_id validation
            return PyToolkit::unloadIndex(fixture, object_id.m_typed_addr, object_id.m_instance_id);
        } 

        PyErr_SetString(PyExc_TypeError, "Invalid object ID");
        return NULL;
    }
    
    PyObject *fetchSingletonObject(db0::swine_ptr<Fixture> &fixture, PyTypeObject *py_type)
    {
        auto &class_factory = fixture->get<db0::object_model::ClassFactory>();
        // find class associated class with the ClassFactory
        auto type = class_factory.getExistingType(py_type);
        if (!type->isSingleton()) {
            THROWF(db0::InputException) << "Not a DBZero singleton type";
        }

        if (!type->isExistingSingleton()) {
            THROWF(db0::InputException) << "Singleton instance does not exist";
        }

        MemoObject *memo_obj = reinterpret_cast<MemoObject*>(py_type->tp_alloc(py_type, 0));
        type->unloadSingleton(&memo_obj->ext());
        return memo_obj;
    }
    
    PyObject *fetchSingletonObject(PyTypeObject *py_type)
    {
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture();
        return fetchSingletonObject(fixture, py_type);
    }
    
    PyObject *fetchSingletonObject(db0::WorkspaceView &view, PyTypeObject *py_type)
    {
        auto uuid = PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture()->getUUID();
        auto fixture = view.getFixture(uuid);
        return fetchSingletonObject(fixture, py_type);
    }
    
    void renameField(PyTypeObject *py_type, const char *from_name, const char *to_name)
    {
        assert(from_name);
        assert(to_name);
        using ClassFactory = db0::object_model::ClassFactory;

        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture();
        auto &class_factory = fixture->get<ClassFactory>();
        // resolve existing DB0 type from python type
        auto type = class_factory.getExistingType(py_type);
        type->renameField(from_name, to_name);
    }
    
#ifndef NDEBUG    

    PyObject *writeBytes(PyObject *self, PyObject *args)
    {
        // parse object size from args
        int size;
        if (!PyArg_ParseTuple(args, "i", &size)) {
            return NULL;
        }
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture();
        // generate a random vector
        std::vector<std::byte> data(size);
        std::generate(data.begin(), data.end(), []() { return (std::byte)(std::rand() % 256); });
        // write bytes by creating a binary object
        db0::v_object<db0::o_binary>(*fixture, data);
        Py_RETURN_NONE;
    }

#endif
}