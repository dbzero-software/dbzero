// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "Migration.hpp"
#include "PySafeAPI.hpp"

namespace db0::python

{

    Migration::Migration(PyObject *py_migrate, std::vector<std::string> &&vars)
        : m_migrate(py_migrate)
        , m_vars(std::move(vars))
    {
    }

    std::vector<Migration> extractMigrations(PyObject *py_migrations)
    {
        if (!py_migrations || py_migrations == Py_None) {
            return {};
        }

        // callable + init vars
        std::vector<Migration> result;        
        if (!PyList_Check(py_migrations)) {
            THROWF(db0::InputException) << "Expected list of tuples";
        }
        Py_ssize_t size = PyList_Size(py_migrations);
        for (Py_ssize_t i = 0; i < size; ++i) {
            PyObject *item = PyList_GetItem(py_migrations, i);
            if (!PyTuple_Check(item) || PyTuple_Size(item) != 2) {
                THROWF(db0::InputException) << "Expected list of tuples";
            }
            PyObject *py_callable = PyTuple_GetItem(item, 0);
            PyObject *py_vars = PyTuple_GetItem(item, 1);
            if (!PyCallable_Check(py_callable) || !PyList_Check(py_vars)) {
                THROWF(db0::InputException) << "Expected list of tuples";
            }
            std::vector<std::string> vars;
            Py_ssize_t vars_size = PyList_Size(py_vars);
            for (Py_ssize_t j = 0; j < vars_size; ++j) {
                PyObject *var = PyList_GetItem(py_vars, j);
                if (!PyUnicode_Check(var)) {
                    THROWF(db0::InputException) << "Expected list of strings";
                }
                vars.push_back(PyUnicode_AsUTF8(var));
            }
            result.emplace_back(py_callable, std::move(vars));
        }
        return result;
    }
    
    PyObject *Migration::exec(PyObject *py_self) const
    {
        auto py_args = Py_OWN(PyTuple_New(1));
        if (!py_args) {
            return nullptr;
        }
        
        PySafeTuple_SetItem(*py_args, 0, Py_BORROW(py_self));
        auto kwargs = Py_OWN(PyDict_New());
        if (!kwargs) {
            return nullptr;
        }
        
        return PyObject_Call(*m_migrate, *py_args, *kwargs);
    }
    
}