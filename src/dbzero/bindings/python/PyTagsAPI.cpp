// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyTagsAPI.hpp"
#include "PyInternalAPI.hpp"
#include "PySnapshot.hpp"
#include <dbzero/object_model/tags/SplitIterator.hpp>
#include <dbzero/object_model/tags/TagIndex.hpp>
#include <dbzero/bindings/python/iter/PyObjectIterable.hpp>
#include <dbzero/bindings/python/iter/PyObjectIterator.hpp>
#include <dbzero/bindings/python/iter/PyJoinIterable.hpp>
#include <dbzero/bindings/python/iter/PyJoinIterator.hpp>
#include <dbzero/bindings/python/types/PyEnum.hpp>
#include <dbzero/object_model/tags/SelectModified.hpp>
#include <dbzero/workspace/Snapshot.hpp>
#include <dbzero/workspace/Workspace.hpp>

namespace db0::python

{

    PyObject *findIn(db0::Snapshot &snapshot, PyObject* const *args, Py_ssize_t nargs,
        PyObject *context, const char *prefix_name)
    {
        using ObjectIterable = db0::object_model::ObjectIterable;
        using TagIndex = db0::object_model::TagIndex;
        using Class = db0::object_model::Class;
        
        std::vector<PyObject*> find_args;
        bool no_result = false;
        std::shared_ptr<Class> type;
        PyTypeObject *lang_type = nullptr;
        auto fixture = db0::object_model::getFindParams(
            snapshot, args, nargs, find_args, type, lang_type, no_result, prefix_name
        );
        fixture->refreshIfUpdated();
        auto &tag_index = fixture->get<TagIndex>();
        std::vector<std::unique_ptr<db0::object_model::QueryObserver> > query_observers;
        auto query_iterator = tag_index.find(find_args.data(), find_args.size(), type, query_observers, no_result);
        auto iter_obj = PyObjectIterableDefault_new();
        iter_obj->makeNew(fixture, std::move(query_iterator), type, lang_type, std::move(query_observers));
        if (context) {
            (iter_obj.get())->ext().attachContext(context);
        }
        return iter_obj.steal();
    }

    using QueryIterator = typename db0::object_model::ObjectIterator::QueryIterator;
    using QueryObserver = db0::object_model::QueryObserver;
    using SplitIterable = db0::object_model::SplitIterable;
    using SplitIterator = db0::object_model::SplitIterator;

    std::pair<std::unique_ptr<QueryIterator>, std::vector<std::unique_ptr<QueryObserver> > >
    splitBy(PyObject *py_tag_list, const ObjectIterable &iterable, bool exclusive)
    {        
        std::vector<std::unique_ptr<QueryObserver> > query_observers;
        auto query = iterable.beginFTQuery(query_observers, -1);
        auto &tag_index = iterable.getFixture()->get<db0::object_model::TagIndex>();
        auto result = tag_index.splitBy(py_tag_list, std::move(query), exclusive);
        query_observers.push_back(std::move(result.second));
        return { std::move(result.first), std::move(query_observers) };
    }
    
    PyObject *trySplitBy(PyObject *py_tags, PyObject *py_query, bool exclusive)
    {
        if (!PyObjectIterable_Check(py_query)) {
            THROWF(db0::InputException) << "Invalid argument type";
        }
        
        auto &iter = reinterpret_cast<PyObjectIterable*>(py_query)->modifyExt();
        auto split_query = splitBy(py_tags, iter, exclusive);
        auto py_iter = PyObjectIterableDefault_new();
        py_iter->makeNew(iter, std::move(split_query.first), std::move(split_query.second), iter.getFilters());
        return py_iter.steal();
    }
    
    PyObject *PyAPI_splitBy(PyObject *, PyObject *args, PyObject *kwargs)
    {
        // extract 2 object arguments
        PyObject *py_tags = nullptr;
        PyObject *py_query = nullptr;
        int exclusive = true;
        // tags, query, exclusive (bool)
        static const char *kwlist[] = {"tags", "query", "exclusive", NULL};
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|p", const_cast<char**>(kwlist), &py_tags, &py_query, &exclusive)) {            
            return NULL;
        }
        
        PY_API_FUNC
        return runSafe(trySplitBy, py_tags, py_query, exclusive);
    } 
    
    PyObject *trySelectModCandidates(const ObjectIterable &iterable, StateNumType from_state,
        std::optional<StateNumType> to_state)
    {
        std::vector<std::unique_ptr<QueryObserver> > query_observers;
        auto fixture = iterable.getFixture();
        // use the last finalized state if the scope upper bound not provided
        if (!to_state) {
            to_state = fixture->getPrefix().getStateNum(true);
        }
        if (to_state < from_state) {
            THROWF(db0::InputException) << "Invalid state range: " << from_state << " - " << *to_state;
        }
        assert(to_state);
        auto &storage = fixture->getPrefix().getStorage();
        auto result_query = db0::object_model::selectModCandidates(
            iterable.beginFTQuery(query_observers, -1), storage, from_state, *to_state
        );
        auto py_iter = PyObjectIterableDefault_new();
        py_iter->makeNew(fixture, std::move(result_query), iterable.getType(), iterable.getLangType(), 
            std::move(query_observers), iterable.getFilters()
        );
        return py_iter.steal();
    }
    
    PyObject *PyAPI_selectModCandidates(PyObject *, PyObject *args, PyObject *kwargs)
    {
        PY_API_FUNC
        PyObject *py_iter = nullptr;
        PyObject *py_scope = nullptr;
        const char * const kwlist[] = {"query", "scope", nullptr};
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|", const_cast<char**>(kwlist), &py_iter, &py_scope)) {
            return nullptr;
        }
        
        if (!PyObjectIterable_Check(py_iter)) {
            THROWF(db0::InputException) << "Invalid argument type";
        }
        
        auto &iter = reinterpret_cast<PyObjectIterable*>(py_iter)->modifyExt();
        // py_scope must be a tuple with 1st element int
        // and 2nd element string
        if (!py_scope || !PyTuple_Check(py_scope) || PyTuple_Size(py_scope) != 2) {
            THROWF(db0::InputException) << "Invalid argument type";
        }

        assert(py_scope);
        PyObject *py_from_state = PyTuple_GetItem(py_scope, 0);
        PyObject *py_to_state = PyTuple_GetItem(py_scope, 1);
        
        StateNumType from_state = 0;
        std::optional<StateNumType> to_state;

        if (PyLong_Check(py_from_state)) {
            from_state = PyLong_AsUnsignedLong(py_from_state);
        } else {
            THROWF(db0::InputException) << "Invalid argument type";            
        }
        
        if (PyLong_Check(py_to_state)) {
            to_state = PyLong_AsUnsignedLong(py_to_state);
        } else if (py_to_state != Py_None) {
            THROWF(db0::InputException) << "Invalid argument type";            
        }
        
        return runSafe(trySelectModCandidates, iter, from_state, to_state);
    }
    
    PyObject *trySplitBySnapshots(const ObjectIterable &iter, const std::vector<db0::Snapshot*> &snapshots)
    {
        auto fixture = iter.getFixture();
        std::vector<std::unique_ptr<QueryObserver> > query_observers;
        auto query = iter.beginFTQuery(query_observers, -1);
        std::vector<db0::swine_ptr<db0::Fixture> > split_fixtures;
        // resolve split fixtures from snapshots
        auto fixture_uuid = fixture->getUUID();
        for (auto snapshot : snapshots) {
            auto fixture = snapshot->getFixture(fixture_uuid, AccessType::READ_ONLY);
            assert(fixture);
            split_fixtures.push_back(fixture);
        }
        
        auto py_iter = PyObjectIterableDefault_new();
        py_iter->makeNewAs<SplitIterable>(fixture, split_fixtures, std::move(query),  iter.getType(), iter.getLangType(), 
            std::move(query_observers), iter.getFilters()
        );
        return py_iter.steal();
    }

    PyObject *PyAPI_splitBySnapshots(PyObject *, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs < 3) {
            PyErr_SetString(PyExc_TypeError, "splitBySnapshots requires at least 3 arguments");
            return NULL;
        }

        auto py_iter = args[0];
        if (!PyObjectIterable_Check(py_iter)) {
            THROWF(db0::InputException) << "Invalid argument type";
        }
        
        auto &iter = reinterpret_cast<PyObjectIterable*>(py_iter)->modifyExt();
        std::vector<db0::Snapshot*> snapshots;
        // collect snapshots from args
        for (Py_ssize_t i = 1; i < nargs; ++i) {
            auto py_snapshot = args[i];
            if (!PySnapshot_Check(py_snapshot)) {
                PyErr_SetString(PyExc_TypeError, "Invalid argument type");
                return NULL;
            }
            auto &snapshot = reinterpret_cast<PySnapshotObject*>(py_snapshot)->modifyExt();
            snapshots.push_back(&snapshot);
        }

        return runSafe(trySplitBySnapshots, iter, snapshots);
    }

    PyObject *trySerialize(PyObject *py_serializable)
    {
        using TypeId = db0::bindings::TypeId;
        std::vector<std::byte> bytes;
        auto type_id = PyToolkit::getTypeManager().getTypeId(py_serializable);
        db0::serial::write(bytes, type_id);
        
        if (type_id == TypeId::OBJECT_ITERABLE) {
            reinterpret_cast<PyObjectIterable*>(py_serializable)->ext().serialize(bytes);
        } else if (type_id == TypeId::DB0_ENUM_VALUE) {
            reinterpret_cast<PyEnumValue*>(py_serializable)->ext().serialize(bytes);
        } else if (type_id == TypeId::DB0_ENUM_VALUE_REPR) {
            reinterpret_cast<PyEnumValueRepr*>(py_serializable)->ext().serialize(bytes);
        } else {
            THROWF(db0::InputException) << "Unsupported or non-serializable type: " 
                << static_cast<int>(type_id) << THROWF_END;
        }
                
        return PyBytes_FromStringAndSize(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    }
    
    PyObject *PyAPI_serialize(PyObject *, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "serialize requires exactly 1 argument");
            return NULL;
        }
        return runSafe(trySerialize, args[0]);
    }
    
    PyObject *tryDeserialize(db0::Snapshot *workspace, PyObject *py_bytes)
    {
        using TypeId = db0::bindings::TypeId;

        if (!PyBytes_Check(py_bytes)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type (expected bytes)");
            return NULL;
        }

        Py_ssize_t size;
        char *data = nullptr;
        PyBytes_AsStringAndSize(py_bytes, &data, &size);
        // extract bytes
        std::vector<std::byte> bytes(size);
        std::copy(data, data + size, reinterpret_cast<char*>(bytes.data()));
        
        auto fixture = workspace->getCurrentFixture();
        auto iter = bytes.cbegin(), end = bytes.cend();
        auto type_id = db0::serial::read<TypeId>(iter, end);
        if (type_id == TypeId::OBJECT_ITERABLE) {
            return PyToolkit::deserializeObjectIterable(fixture, iter, end).steal();
        } else if (type_id == TypeId::DB0_ENUM_VALUE) {
            // NOTICE: this function may return EnumValue or EnumValueRepr
            return PyToolkit::deserializeEnumValue(fixture, iter, end).steal();
        } else if (type_id == TypeId::DB0_ENUM_VALUE_REPR) {
            return PyToolkit::deserializeEnumValueRepr(fixture, iter, end).steal();
        } else {
            THROWF(db0::InputException) << "Unsupported serialized type id: " 
                << static_cast<int>(type_id) << THROWF_END;
        }
    }
    
    PyObject *PyAPI_deserialize(PyObject *, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "deserialize requires exactly 1 argument");
            return NULL;
        }        
        auto &workspace = PyToolkit::getPyWorkspace().getWorkspace();
        return runSafe(tryDeserialize, &workspace, args[0]);
    }
    
    PyObject *joinIn(db0::Snapshot &snapshot, PyObject* const *args, Py_ssize_t nargs,
        PyObject *join_on_arg, PyObject *context, const char *prefix_name)
    {
        assert(join_on_arg);
        using JoinIterable = db0::object_model::JoinIterable;
        using ObjectIterable = db0::object_model::ObjectIterable;
        using TagIndex = db0::object_model::TagIndex;
        using Class = db0::object_model::Class;
        
        // join args are either types or ObjectIterables
        std::vector<std::shared_ptr<Class> > types;
        std::vector<PyTypeObject*> lang_types;
        // the object iterables to join
        std::vector<const ObjectIterable*> object_iterables;
        // iterators persistency buffer
        std::vector<std::unique_ptr<ObjectIterable> > iter_buf;
        const ObjectIterable *tag_iterable = nullptr;
        auto fixture = db0::object_model::getJoinParams(
            snapshot, args, nargs, join_on_arg, object_iterables, tag_iterable, iter_buf, prefix_name
        );
        
        // collect types and lang_types
        {
            for (auto obj_iter : object_iterables) {
                types.push_back(obj_iter->getType());
                lang_types.push_back(obj_iter->getLangType());
            }
        }

        fixture->refreshIfUpdated();
        auto &tag_index = fixture->get<TagIndex>();        
        auto query_iterator = tag_index.makeTagProduct(object_iterables, tag_iterable);
        auto iter_obj = PyJoinIterableDefault_new();
        iter_obj->makeNew(fixture, std::move(query_iterator), std::move(types), lang_types);
        if (context) {
            (iter_obj.get())->ext().attachContext(context);
        }
        return iter_obj.steal();
    }
    
}