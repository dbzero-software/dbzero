#include "PyTagsAPI.h"
#include <dbzero/object_model/tags/SplitIterator.hpp>

namespace db0::python

{

    PyObject *findIn(db0::Snapshot &snapshot, PyObject* const *args, Py_ssize_t nargs, PyObject *context)
    {
        using ObjectIterable = db0::object_model::ObjectIterable;
        using TagIndex = db0::object_model::TagIndex;
        using Class = db0::object_model::Class;
        
        std::vector<PyObject*> find_args;
        bool no_result = false;
        std::shared_ptr<Class> type;
        PyTypeObject *lang_type = nullptr;
        auto fixture = db0::object_model::getFindParams(snapshot, args, nargs, find_args, type, lang_type, no_result);
        fixture->refreshIfUpdated();
        auto &tag_index = fixture->get<TagIndex>();
        std::vector<std::unique_ptr<db0::object_model::QueryObserver> > query_observers;
        auto query_iterator = tag_index.find(find_args.data(), find_args.size(), type, query_observers, no_result);
        auto iter_obj = PyObjectIterableDefault_new();
        ObjectIterable::makeNew(&(iter_obj.get())->modifyExt(), fixture, std::move(query_iterator), type,
            lang_type, std::move(query_observers));
        if (context) {
            (iter_obj.get())->ext().attachContext(context);
        }
        return iter_obj.steal();
    }

    using QueryIterator = typename db0::object_model::ObjectIterator::QueryIterator;
    using QueryObserver = db0::object_model::QueryObserver;

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
    
    PyObject *trySplitBy(PyObject *args, PyObject *kwargs)
    {
        // extract 2 object arguments
        PyObject *py_tags = nullptr;
        PyObject *py_query = nullptr;
        int exclusive = true;
        // tags, query, exclusive (bool)
        static const char *kwlist[] = {"tags", "query", "exclusive", NULL};
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|p", const_cast<char**>(kwlist), &py_tags, &py_query, &exclusive)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }
        
        if (!PyObjectIterable_Check(py_query)) {
            THROWF(db0::InputException) << "Invalid argument type";
        }
        
        auto &iter = reinterpret_cast<PyObjectIterable*>(py_query)->modifyExt();
        auto split_query = splitBy(py_tags, iter, exclusive);
        auto py_iter = PyObjectIterableDefault_new();
        iter.makeNew(&(py_iter.get())->modifyExt(), std::move(split_query.first), std::move(split_query.second),
            iter.getFilters());
        return py_iter.steal();
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
        ObjectIterable::makeNew(&(py_iter.get())->modifyExt(), fixture, std::move(result_query), iterable.getType(),
            iterable.getLangType(), {}, iterable.getFilters()
        );
        return py_iter.steal();
    }
    
    PyObject *trySplitBySnapshots(const ObjectIterable &iter, const std::vector<db0::Snapshot*> &snapshots)
    {
        auto fixture = iterable.getFixture();
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
        SplitIterable::makeNew(&(py_iter.get())->modifyExt(), fixture, std::move(result_query), iterable.getType(),
            iterable.getLangType(), {}, iterable.getFilters()
        );
        return py_iter.steal();
    }
    
}