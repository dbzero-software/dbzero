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

    template <> int64_t get_py_hash_impl<TypeId::STRING>(PyObject *key)
    {
        auto unicode_value = PyUnicode_AsUTF8(key);
        return std::hash<std::string>{}(unicode_value);
    }

    template <> int64_t get_py_hash_impl<TypeId::BYTES>(PyObject *key) 
    {
        auto bytes_value = PyBytes_AsString(key);
        return std::hash<std::string>{}(bytes_value);
    }

    template <> int64_t get_py_hash_impl<TypeId::TUPLE>(PyObject *key) 
    {
        auto tuple_size = PyTuple_Size(key);
        int64_t hash = 0;
        for (int i = 0; i < tuple_size; ++i) {
            auto item = PyTuple_GetItem(key, i);
            hash ^= get_py_hash(item);
        }
        return hash;
    }

    template <> int64_t get_py_hash_impl<TypeId::DB0_TUPLE>(PyObject *key) 
    {
        TupleObject *tuple_obj = reinterpret_cast<TupleObject*>(key);
        int64_t hash = 0;
        for (size_t i = 0; i < tuple_obj->ext().getData()->size(); ++i) {
            tuple_obj->ext().getFixture()->refreshIfUpdated();
            auto *item = tuple_obj->ext().getItem(i).get();
            hash ^= get_py_hash(item);
        }
        return hash;
    }
    
    template <> int64_t get_py_hash_impl<TypeId::DB0_ENUM_VALUE>(PyObject *key) 
    {
        auto enum_value = PyToolkit::getTypeManager().extractEnumValue(key);
        return enum_value.getUID().asULong();
    }

    template <> int64_t get_py_hash_impl<TypeId::MEMO_OBJECT>(PyObject *key)
    {
        return reinterpret_cast<MemoObject*>(key)->ext().getAddress();
    }


    std::int64_t get_py_hash_impl_for_simple_obj(PyObject *key) {
        return PyToolkit::getTypeManager().extractUInt64(key);
    }

    std::int64_t get_py_hash_impl_default(PyObject *key) {
        return PyObject_Hash(key);
    }

    void registerGetHashFunctions(std::vector<PyHashFunct> &functions)
    {
        functions.resize(static_cast<int>(TypeId::COUNT));
        std::fill(functions.begin(), functions.end(), nullptr);
        functions[static_cast<int>(TypeId::STRING)] = get_py_hash_impl<TypeId::STRING>;
        functions[static_cast<int>(TypeId::BYTES)]  = get_py_hash_impl<TypeId::BYTES>;
        functions[static_cast<int>(TypeId::DB0_TUPLE)] = get_py_hash_impl<TypeId::DB0_TUPLE>;
        functions[static_cast<int>(TypeId::TUPLE)] = get_py_hash_impl<TypeId::TUPLE>;
        functions[static_cast<int>(TypeId::DB0_ENUM_VALUE)] = get_py_hash_impl<TypeId::DB0_ENUM_VALUE>;
        functions[static_cast<int>(TypeId::MEMO_OBJECT)] = get_py_hash_impl<TypeId::MEMO_OBJECT>;
        functions[static_cast<int>(TypeId::DB0_CLASS)] = get_py_hash_impl_for_simple_obj;

        functions[static_cast<int>(TypeId::DATETIME)] = get_py_hash_impl_for_simple_obj;
        functions[static_cast<int>(TypeId::DATETIME_TZ)] = get_py_hash_impl_for_simple_obj;
        functions[static_cast<int>(TypeId::DATE)] = get_py_hash_impl_for_simple_obj;
        functions[static_cast<int>(TypeId::TIME)] = get_py_hash_impl_for_simple_obj;
        functions[static_cast<int>(TypeId::TIME_TZ)] = get_py_hash_impl_for_simple_obj;
        functions[static_cast<int>(TypeId::INTEGER)] = get_py_hash_impl_for_simple_obj;
        functions[static_cast<int>(TypeId::DECIMAL)] = get_py_hash_impl_for_simple_obj;

    }

    PyObject* get_py_hash_as_py_object(PyObject *key) {
        return PyLong_FromLong(get_py_hash(key));
    }

    std::int64_t get_py_hash(PyObject *key)
    {
        static std::vector<PyHashFunct> get_py_hash_functions;
        if (get_py_hash_functions.empty()) {
            registerGetHashFunctions(get_py_hash_functions);
        }

        auto type_id = PyToolkit::getTypeManager().getTypeId(key);
        assert(static_cast<int>(type_id) < get_py_hash_functions.size());
        auto func_ptr = get_py_hash_functions[static_cast<int>(type_id)];
        if (!func_ptr) {
            return get_py_hash_impl_default(key);
        }
        return func_ptr(key);
    }
        
}