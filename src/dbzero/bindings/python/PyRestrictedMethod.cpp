// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyRestrictedMethod.hpp"
#include "PyToolkit.hpp"
#include "PySafeAPI.hpp"
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/workspace/Config.hpp>
#include <dbzero/core/memory/config.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <object.h>
#include <cstring>
#include <cstddef>
#include <vector>

#ifndef PYVAROBJECT_HEAD_INIT_DESIGNATED
#define PYVAROBJECT_HEAD_INIT_DESIGNATED \
    .ob_base = { \
        .ob_base = { \
            .ob_refcnt = 1, \
            .ob_type = NULL, \
        }, \
        .ob_size = 0, \
    }
#endif

namespace db0::python

{
    thread_local bool g_restricted_memo_init = false;
    thread_local std::size_t g_restricted_memo_user_code_depth = 0;

    class RestrictedContextResource
    {
    public:
        explicit RestrictedContextResource(PyTypes::ObjectPtr restricted_context)
        {
            set(restricted_context);
        }

        void set(PyTypes::ObjectPtr restricted_context)
        {
            if (restricted_context && restricted_context != Py_None) {
                m_restricted_context = Py_BORROW(restricted_context);
            } else {
                m_restricted_context.reset();
            }
        }

        PyTypes::ObjectPtr get() const
        {
            return m_restricted_context.get();
        }

        bool hasContext() const
        {
            return m_restricted_context.get() != nullptr;
        }

    private:
        PyTypes::ObjectSharedPtr m_restricted_context;
    };

    struct PyRestrictedMethod
    {
        PyObject_HEAD
        PyObject *m_method = nullptr;
    };

    PyObject *PyRestrictedMethod_call(PyRestrictedMethod *self, PyObject *args, PyObject *kwargs)
    {
        ScopedRestrictedMemoUserCode user_code;
        return PyObject_Call(self->m_method, args, kwargs);
    }

    PyObject *PyRestrictedMethod_getattro(PyRestrictedMethod *, PyObject *attr)
    {
        const char *attr_name = PyUnicode_AsUTF8(attr);
        if (!attr_name) {
            PyErr_SetString(PyExc_AttributeError, "Invalid attribute name");
            return nullptr;
        }
        PyErr_Format(PyExc_AttributeError, "Restricted method attribute access denied: %s", attr_name);
        return nullptr;
    }

    PyObject *PyRestrictedMethod_dir(PyRestrictedMethod *, PyObject *)
    {
        PyErr_SetString(PyExc_AttributeError, "Restricted method directory access denied");
        return nullptr;
    }

    void PyRestrictedMethod_dealloc(PyRestrictedMethod *self)
    {
        Py_XDECREF(self->m_method);
        Py_TYPE(self)->tp_free(reinterpret_cast<PyObject *>(self));
    }

    static PyMethodDef PyRestrictedMethod_methods[] = {
        {"__dir__", reinterpret_cast<PyCFunction>(PyRestrictedMethod_dir), METH_NOARGS, nullptr},
        {nullptr}
    };

    PyTypeObject PyRestrictedMethodType = {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "RestrictedMethod",
        .tp_basicsize = sizeof(PyRestrictedMethod),
        .tp_itemsize = 0,
        .tp_dealloc = reinterpret_cast<destructor>(PyRestrictedMethod_dealloc),
        .tp_call = reinterpret_cast<ternaryfunc>(PyRestrictedMethod_call),
        .tp_getattro = reinterpret_cast<getattrofunc>(PyRestrictedMethod_getattro),
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "restricted dbzero memo method proxy",
        .tp_methods = PyRestrictedMethod_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = PyType_GenericNew,
        .tp_free = PyObject_Free,
    };

    ScopedRestrictedMemoInit::ScopedRestrictedMemoInit()
        : m_was_enabled(g_restricted_memo_init)
    {
        g_restricted_memo_init = true;
    }

    ScopedRestrictedMemoInit::~ScopedRestrictedMemoInit()
    {
        g_restricted_memo_init = m_was_enabled;
    }

    ScopedRestrictedMemoUserCode::ScopedRestrictedMemoUserCode()
        : m_previous_depth(g_restricted_memo_user_code_depth)
    {
        ++g_restricted_memo_user_code_depth;
    }

    ScopedRestrictedMemoUserCode::~ScopedRestrictedMemoUserCode()
    {
        g_restricted_memo_user_code_depth = m_previous_depth;
    }

    PyObject *makeRestrictedMethod(PyObject *method)
    {
        auto result = reinterpret_cast<PyRestrictedMethod *>(PyRestrictedMethodType.tp_alloc(&PyRestrictedMethodType, 0));
        if (!result) {
            return nullptr;
        }
        Py_INCREF(method);
        result->m_method = method;
        return reinterpret_cast<PyObject *>(result);
    }

    bool isRestrictedName(const char *attr_name)
    {
        return attr_name[0] == '_';
    }

    bool isRestrictedMemoContextActive()
    {
        return g_restricted_memo_init || g_restricted_memo_user_code_depth > 0;
    }

    void RestrictedContextManager::clear()
    {
        m_default_context.reset();
        m_prefix_contexts.clear();
    }

    void RestrictedContextManager::initDefault(db0::Workspace &workspace, ObjectPtr restricted_context)
    {
        setDefaultContext(workspace, restricted_context);
    }

    RestrictedContextManager::RestrictionLevel RestrictedContextManager::getDefaultLevel(
        const db0::Workspace &workspace) const
    {
        if (workspace.isDefaultRestricted()) {
            return RestrictionLevel::statically_restricted;
        }
        if (workspace.isDefaultRestrictedCtx()) {
            return RestrictionLevel::context;
        }
        return RestrictionLevel::unrestricted;
    }

    RestrictedContextManager::RestrictionLevel RestrictedContextManager::getFixtureLevel(
        const db0::Fixture &fixture) const
    {
        if (fixture.isRestricted()) {
            return RestrictionLevel::statically_restricted;
        }
        if (fixture.isRestrictedCtx()) {
            return RestrictionLevel::context;
        }
        return RestrictionLevel::unrestricted;
    }

    RestrictedContextManager::RestrictionLevel RestrictedContextManager::getRequestedLevel(
        std::optional<bool> restricted, ObjectPtr restricted_context, bool restricted_context_given) const
    {
        if (restricted && !*restricted) {
            THROWF(db0::InputException) << "restricted mode cannot be weakened";
        }
        if (restricted && *restricted) {
            if (restricted_context_given && restricted_context && restricted_context != Py_None) {
                THROWF(db0::InputException) << "restricted=True cannot be combined with restricted_context";
            }
            return RestrictionLevel::statically_restricted;
        }
        if (restricted_context_given && restricted_context && restricted_context != Py_None) {
            return RestrictionLevel::context;
        }
        return RestrictionLevel::unrestricted;
    }

    RestrictedContextManager::ObjectPtr RestrictedContextManager::getPrefixContext(const std::string &prefix_name) const
    {
        auto it = m_prefix_contexts.find(prefix_name);
        if (it != m_prefix_contexts.end()) {
            return it->second.get();
        }
        return nullptr;
    }

    void RestrictedContextManager::validateUpgrade(RestrictionLevel current_level, ObjectPtr current_context,
        RestrictionLevel requested_level, ObjectPtr requested_context) const
    {
        if (requested_level == RestrictionLevel::unrestricted) {
            if (current_level != RestrictionLevel::unrestricted) {
                THROWF(db0::InputException) << "restricted mode cannot be weakened";
            }
            return;
        }
        if (current_level == RestrictionLevel::statically_restricted &&
            requested_level != RestrictionLevel::statically_restricted)
        {
            THROWF(db0::InputException) << "static restricted mode cannot be changed to context restricted mode";
        }
        if (current_level == RestrictionLevel::context && requested_level == RestrictionLevel::context &&
            current_context != requested_context)
        {
            THROWF(db0::InputException) << "restricted_context cannot be replaced";
        }
    }

    void RestrictedContextManager::setDefaultContext(db0::Workspace &workspace, ObjectPtr restricted_context)
    {
        if (restricted_context && restricted_context != Py_None) {
            db0::Settings::markRestrictedAccessUsed();
            m_default_context = Py_BORROW(restricted_context);
        } else {
            m_default_context.reset();
        }
        workspace.setDefaultRestrictedCtx(m_default_context.get() != nullptr);
    }

    void RestrictedContextManager::setPrefixContext(db0::Workspace &workspace, const std::string &prefix_name,
        ObjectPtr restricted_context)
    {
        if (restricted_context && restricted_context != Py_None) {
            db0::Settings::markRestrictedAccessUsed();
            m_prefix_contexts[prefix_name] = Py_BORROW(restricted_context);
        } else {
            m_prefix_contexts.erase(prefix_name);
        }
        syncFixture(workspace, prefix_name);
    }

    void RestrictedContextManager::syncFixture(db0::Workspace &workspace, const std::string &prefix_name) const
    {
        auto fixture = workspace.tryFindFixture(prefix_name);
        if (!!fixture) {
            db0::python::setFixtureRestrictedContext(fixture, fixture->isRestricted() ? nullptr : getEffectiveContext(*fixture));
        }
    }

    void RestrictedContextManager::syncAllFixtures(db0::Workspace &workspace) const
    {
        std::vector<std::string> prefix_names;
        workspace.forEachFixture([&prefix_names](const db0::Fixture &fixture) {
            prefix_names.push_back(fixture.getPrefix().getName());
            return true;
        });
        for (auto &prefix_name: prefix_names) {
            syncFixture(workspace, prefix_name);
        }
    }

    RestrictedContextManager::ObjectPtr RestrictedContextManager::getEffectiveContext(const db0::Fixture &fixture) const
    {
        auto prefix_context = getPrefixContext(fixture.getPrefix().getName());
        return prefix_context ? prefix_context : m_default_context.get();
    }

    void RestrictedContextManager::setConfigRestricted(std::shared_ptr<db0::Config> config, bool restricted) const
    {
        if (!config) {
            return;
        }
        PySafeDict_SetItemString(config->getRawConfig().get(), "restricted", Py_BORROW(restricted ? Py_True : Py_False));
    }

    void RestrictedContextManager::setRestricted(db0::Workspace &workspace, std::shared_ptr<db0::Config> config,
        std::optional<bool> restricted, ObjectPtr restricted_context, bool restricted_context_given,
        const std::optional<std::string> &prefix_name)
    {
        auto requested_level = getRequestedLevel(restricted, restricted_context, restricted_context_given);
        if (prefix_name) {
            auto fixture = workspace.tryFindFixture(*prefix_name);
            if (!fixture) {
                THROWF(db0::InputException) << "Prefix is not open: " << *prefix_name;
            }
            validateUpgrade(getFixtureLevel(*fixture), getEffectiveContext(*fixture), requested_level, restricted_context);
            if (requested_level == RestrictionLevel::statically_restricted) {
                fixture->setRestricted(true);
                setPrefixContext(workspace, *prefix_name, nullptr);
            } else if (requested_level == RestrictionLevel::context) {
                setPrefixContext(workspace, *prefix_name, restricted_context);
            }
            return;
        }

        validateUpgrade(getDefaultLevel(workspace), m_default_context.get(), requested_level, restricted_context);
        if (requested_level == RestrictionLevel::statically_restricted) {
            workspace.setDefaultRestricted(true);
            setDefaultContext(workspace, nullptr);
            setConfigRestricted(config, true);
            std::vector<std::string> prefix_names;
            workspace.forEachFixture([&prefix_names](const db0::Fixture &fixture) {
                prefix_names.push_back(fixture.getPrefix().getName());
                return true;
            });
            for (auto &name: prefix_names) {
                auto fixture = workspace.tryFindFixture(name);
                if (!!fixture) {
                    fixture->setRestricted(true);
                }
            }
            syncAllFixtures(workspace);
        } else if (requested_level == RestrictionLevel::context) {
            setDefaultContext(workspace, restricted_context);
            syncAllFixtures(workspace);
        }
    }

    void RestrictedContextManager::validateOpenRestricted(db0::Workspace &workspace, const std::string &prefix_name,
        std::optional<bool> restricted) const
    {
        if (!restricted || *restricted) {
            return;
        }
        auto fixture = workspace.tryFindFixture(prefix_name);
        if (!!fixture && getFixtureLevel(*fixture) != RestrictionLevel::unrestricted) {
            THROWF(db0::InputException) << "restricted mode cannot be weakened";
        }
    }

    void RestrictedContextManager::applyOpenRestrictedContext(db0::Workspace &workspace, const std::string &prefix_name,
        ObjectPtr restricted_context, bool restricted_context_given, bool is_initial_prefix_config)
    {
        if (!restricted_context_given) {
            syncFixture(workspace, prefix_name);
            return;
        }
        auto fixture = workspace.tryFindFixture(prefix_name);
        if (!fixture) {
            return;
        }
        auto requested_level = restricted_context && restricted_context != Py_None
            ? RestrictionLevel::context : RestrictionLevel::unrestricted;
        if (!is_initial_prefix_config) {
            validateUpgrade(getFixtureLevel(*fixture), getEffectiveContext(*fixture), requested_level, restricted_context);
        }
        if (requested_level == RestrictionLevel::context) {
            setPrefixContext(workspace, prefix_name, restricted_context);
        } else {
            syncFixture(workspace, prefix_name);
        }
    }

    void setFixtureRestrictedContext(db0::swine_ptr<db0::Fixture> &fixture, PyTypes::ObjectPtr restricted_context)
    {
        auto *resource = fixture->tryGet<RestrictedContextResource>();
        if (resource) {
            resource->set(restricted_context);
        } else {
            resource = &fixture->addResource<RestrictedContextResource>(restricted_context);
        }
        fixture->setRestrictedCtx(resource->hasContext());
    }

    bool resolveRestrictedContextVar(PyTypes::ObjectPtr restricted_context)
    {
        if (!restricted_context) {
            return false;
        }

        auto value = Py_OWN(PyObject_CallMethod(restricted_context, "get", nullptr));
        if (!value) {
            if (PyErr_ExceptionMatches(PyExc_LookupError)) {
                PyErr_Clear();
                return false;
            }
            THROWF(db0::InputException) << "restricted_context.get() failed";
        }

        auto is_restricted = PyObject_IsTrue(*value);
        if (is_restricted < 0) {
            THROWF(db0::InputException) << "restricted_context truthiness check failed";
        }
        return is_restricted;
    }

    bool resolveRestrictedCtx(const db0::Fixture &fixture)
    {
        auto *resource = fixture.tryGet<RestrictedContextResource>();
        if (!resource) {
            return false;
        }
        return resolveRestrictedContextVar(resource->get());
    }

    bool isDunderName(const char *attr_name)
    {
        if (attr_name[0] != '_' || attr_name[1] != '_') {
            return false;
        }

        auto len = std::strlen(attr_name);
        return len >= 4 && attr_name[len - 2] == '_' && attr_name[len - 1] == '_';
    }

    PyObject *bindRestrictedMethod(PyObject *memo_obj, PyObject *raw_attr)
    {
        auto method = Py_OWN(PyMethod_New(raw_attr, memo_obj));
        if (!method) {
            return nullptr;
        }
        return makeRestrictedMethod(*method);
    }

    PyObject *tryGetRestrictedClassAttr(PyObject *memo_obj, PyObject *attr, const char *attr_name)
    {
        auto *type = Py_TYPE(memo_obj);
        auto *mro = type->tp_mro;
        if (!mro || !PyTuple_Check(mro)) {
            Py_RETURN_NONE;
        }

        for (Py_ssize_t i = 0; i < PyTuple_GET_SIZE(mro); ++i) {
            auto *mro_item = PyTuple_GET_ITEM(mro, i);
            if (!PyType_Check(mro_item)) {
                continue;
            }
            auto *mro_type = reinterpret_cast<PyTypeObject *>(mro_item);
            if (!mro_type->tp_dict) {
                continue;
            }
            auto *raw_attr = PyDict_GetItemWithError(mro_type->tp_dict, attr);
            if (!raw_attr) {
                if (PyErr_Occurred()) {
                    return nullptr;
                }
                continue;
            }
            if (PyFunction_Check(raw_attr)) {
                if (isRestrictedName(attr_name)) {
                    if (g_restricted_memo_init && std::strcmp(attr_name, "__post_init__") == 0) {
                        return bindRestrictedMethod(memo_obj, raw_attr);
                    }
                    if (g_restricted_memo_user_code_depth > 0 && !isDunderName(attr_name)) {
                        return bindRestrictedMethod(memo_obj, raw_attr);
                    }
                    break;
                }
                return bindRestrictedMethod(memo_obj, raw_attr);
            }
            if (!isRestrictedName(attr_name) && PyDescr_IsData(raw_attr)) {
                auto *descr_get = Py_TYPE(raw_attr)->tp_descr_get;
                if (descr_get) {
                    ScopedRestrictedMemoUserCode user_code;
                    return descr_get(raw_attr, memo_obj, reinterpret_cast<PyObject *>(type));
                }
            }
            break;
        }

        Py_RETURN_NONE;
    }

    PyObject *tryRestrictedMemoGetattro(
        PyObject *memo_obj,
        PyObject *attr,
        const char *attr_name,
        PyTypes::ObjectSharedPtr &member
    )
    {
        if (isRestrictedName(attr_name)) {
            if ((g_restricted_memo_init && std::strcmp(attr_name, "__post_init__") == 0) ||
                (g_restricted_memo_user_code_depth > 0 && !isDunderName(attr_name))) {
                auto restricted_method = Py_OWN(tryGetRestrictedClassAttr(memo_obj, attr, attr_name));
                if (!restricted_method) {
                    return nullptr;
                }
                if (*restricted_method != Py_None) {
                    return restricted_method.steal();
                }
            }

            PyErr_Format(PyExc_AttributeError, "Restricted memo attribute access denied: %s", attr_name);
            return nullptr;
        }

        if (member.get()) {
            return member.steal();
        }

        auto restricted_method = Py_OWN(tryGetRestrictedClassAttr(memo_obj, attr, attr_name));
        if (!restricted_method) {
            return nullptr;
        }
        if (*restricted_method != Py_None) {
            return restricted_method.steal();
        }

        PyErr_Format(PyExc_AttributeError, "Restricted memo attribute access denied: %s", attr_name);
        return nullptr;
    }

}
