#include "PyHash.hpp"
#include <Python.h>
#include <cstring>
#include <string>
#include <vector>
#include <dbzero/bindings/python/collections/PyTuple.hpp>
#include <dbzero/bindings/python/Memo.hpp>
#include <dbzero/object_model/object/Object.hpp>


namespace db0::python

{

    using PyHashFunct = int64_t (*)(PyObject *);

    template <> int64_t getPyHashImpl<TypeId::STRING>(PyObject *key)
    {
        auto unicode_value = PyUnicode_AsUTF8(key);
        return std::hash<std::string>{}(unicode_value);
    }

    template <> int64_t getPyHashImpl<TypeId::BYTES>(PyObject *key) 
    {
        auto bytes_value = PyBytes_AsString(key);
        return std::hash<std::string>{}(bytes_value);
    }
    
    template <> int64_t getPyHashImpl<TypeId::TUPLE>(PyObject *key)
    {
        auto tuple_size = PyTuple_Size(key);
        int64_t hash = 0;
        for (int i = 0; i < tuple_size; ++i) {
            auto item = PyTuple_GetItem(key, i);
            hash ^= getPyHash(item);
        }
        return hash;
    }
    
    template <> int64_t getPyHashImpl<TypeId::DB0_TUPLE>(PyObject *key)
    {
        TupleObject *tuple_obj = reinterpret_cast<TupleObject*>(key);
        std::int64_t hash = 0;
        for (std::size_t i = 0; i < tuple_obj->ext().getData()->size(); ++i) {
            auto item = tuple_obj->ext().getItem(i);
            hash ^= getPyHash(*item);
        }
        return hash;
    }
    
    template <> int64_t getPyHashImpl<TypeId::DB0_ENUM_VALUE>(PyObject *key) {
        return PyToolkit::getTypeManager().extractEnumValue(key).getPermHash();        
    }

    template <> int64_t getPyHashImpl<TypeId::DB0_ENUM_VALUE_REPR>(PyObject *key) {
        return PyToolkit::getTypeManager().extractEnumValueRepr(key).getPermHash();
    }

    template <> int64_t getPyHashImpl<TypeId::MEMO_OBJECT>(PyObject *key) 
    {
        auto &obj = reinterpret_cast<MemoObject*>(key)->ext();
        if (!obj.hasInstance()) {
            THROWF(db0::InputException) << "Memo object is not initialized" << THROWF_END;
        }
        return obj.getAddress().getValue();
    }
    
    std::int64_t getPyHashImpl_for_simple_obj(PyObject *key) {
        return PyToolkit::getTypeManager().extractUInt64(key);
    }

    std::int64_t getPyHashDefaultImpl(PyObject *key) {
        return PyObject_Hash(key);
    }

    void registerGetHashFunctions(std::vector<PyHashFunct> &functions)
    {
        functions.resize(static_cast<int>(TypeId::COUNT));
        std::fill(functions.begin(), functions.end(), nullptr);
        functions[static_cast<int>(TypeId::STRING)] = getPyHashImpl<TypeId::STRING>;
        functions[static_cast<int>(TypeId::BYTES)]  = getPyHashImpl<TypeId::BYTES>;
        functions[static_cast<int>(TypeId::DB0_TUPLE)] = getPyHashImpl<TypeId::DB0_TUPLE>;
        functions[static_cast<int>(TypeId::TUPLE)] = getPyHashImpl<TypeId::TUPLE>;
        functions[static_cast<int>(TypeId::DB0_ENUM_VALUE)] = getPyHashImpl<TypeId::DB0_ENUM_VALUE>;
        functions[static_cast<int>(TypeId::DB0_ENUM_VALUE_REPR)] = getPyHashImpl<TypeId::DB0_ENUM_VALUE_REPR>;
        functions[static_cast<int>(TypeId::MEMO_OBJECT)] = getPyHashImpl<TypeId::MEMO_OBJECT>;
        functions[static_cast<int>(TypeId::DB0_CLASS)] = getPyHashImpl_for_simple_obj;
        functions[static_cast<int>(TypeId::DATETIME)] = getPyHashImpl_for_simple_obj;
        functions[static_cast<int>(TypeId::DATETIME_TZ)] = getPyHashImpl_for_simple_obj;
        functions[static_cast<int>(TypeId::DATE)] = getPyHashImpl_for_simple_obj;
        functions[static_cast<int>(TypeId::TIME)] = getPyHashImpl_for_simple_obj;
        functions[static_cast<int>(TypeId::TIME_TZ)] = getPyHashImpl_for_simple_obj;
        functions[static_cast<int>(TypeId::INTEGER)] = getPyHashImpl_for_simple_obj;
        functions[static_cast<int>(TypeId::DECIMAL)] = getPyHashImpl_for_simple_obj;
    }

    PyObject* getPyHashAsPyObject(PyObject *key) {
        return PyLong_FromLong(getPyHash(key));
    }
    
    std::int64_t getPyHash(PyObject *key)
    {
        static std::vector<PyHashFunct> getPyHash_functions;
        if (getPyHash_functions.empty()) {
            registerGetHashFunctions(getPyHash_functions);
        }

        auto type_id = PyToolkit::getTypeManager().getTypeId(key);
        assert(static_cast<int>(type_id) < getPyHash_functions.size());
        auto func_ptr = getPyHash_functions[static_cast<int>(type_id)];
        if (!func_ptr) {
            return getPyHashDefaultImpl(key);
        }
        return func_ptr(key);
    }
    
}