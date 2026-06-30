// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include "PyTypes.hpp"
#include <dbzero/core/memory/swine_ptr.hpp>

namespace db0
{

    class Config;
    class Fixture;
    class Workspace;

}

namespace db0::python

{

    extern PyTypeObject PyRestrictedMethodType;

    class ScopedRestrictedMemoInit
    {
    public:
        ScopedRestrictedMemoInit();
        ~ScopedRestrictedMemoInit();

        ScopedRestrictedMemoInit(const ScopedRestrictedMemoInit &) = delete;
        ScopedRestrictedMemoInit &operator=(const ScopedRestrictedMemoInit &) = delete;

    private:
        bool m_was_enabled;
    };

    class ScopedRestrictedMemoUserCode
    {
    public:
        ScopedRestrictedMemoUserCode();
        ~ScopedRestrictedMemoUserCode();

        ScopedRestrictedMemoUserCode(const ScopedRestrictedMemoUserCode &) = delete;
        ScopedRestrictedMemoUserCode &operator=(const ScopedRestrictedMemoUserCode &) = delete;

    private:
        std::size_t m_previous_depth;
    };

    bool isRestrictedName(const char *attr_name);
    bool isRestrictedMemoContextActive();

    class RestrictedContextManager
    {
    public:
        using ObjectPtr = PyTypes::ObjectPtr;
        using ObjectSharedPtr = PyTypes::ObjectSharedPtr;

        void clear();
        void initDefault(db0::Workspace &workspace, ObjectPtr restricted_context);
        void setRestricted(db0::Workspace &workspace, std::shared_ptr<db0::Config> config,
            std::optional<bool> restricted, ObjectPtr restricted_context, bool restricted_context_given,
            const std::optional<std::string> &prefix_name = {});
        void validateOpenRestricted(db0::Workspace &workspace, const std::string &prefix_name,
            std::optional<bool> restricted) const;
        void applyOpenRestrictedContext(db0::Workspace &workspace, const std::string &prefix_name,
            ObjectPtr restricted_context, bool restricted_context_given, bool is_initial_prefix_config);
        ObjectPtr getEffectiveContext(const db0::Fixture &fixture) const;
        void syncFixture(db0::Workspace &workspace, const std::string &prefix_name) const;

    private:
        enum class RestrictionLevel {
            unrestricted,
            context,
            statically_restricted
        };

        ObjectSharedPtr m_default_context;
        std::unordered_map<std::string, ObjectSharedPtr> m_prefix_contexts;

        RestrictionLevel getDefaultLevel(const db0::Workspace &workspace) const;
        RestrictionLevel getFixtureLevel(const db0::Fixture &fixture) const;
        RestrictionLevel getRequestedLevel(std::optional<bool> restricted, ObjectPtr restricted_context,
            bool restricted_context_given) const;
        ObjectPtr getPrefixContext(const std::string &prefix_name) const;
        void validateUpgrade(RestrictionLevel current_level, ObjectPtr current_context,
            RestrictionLevel requested_level, ObjectPtr requested_context) const;
        void setDefaultContext(db0::Workspace &workspace, ObjectPtr restricted_context);
        void setPrefixContext(db0::Workspace &workspace, const std::string &prefix_name, ObjectPtr restricted_context);
        void syncAllFixtures(db0::Workspace &workspace) const;
        void setConfigRestricted(std::shared_ptr<db0::Config> config, bool restricted) const;
    };

    void setFixtureRestrictedContext(db0::swine_ptr<db0::Fixture> &fixture, PyTypes::ObjectPtr restricted_context);
    bool resolveRestrictedCtx(const db0::Fixture &fixture);
    PyObject *tryRestrictedMemoGetattro(
        PyObject *memo_obj,
        PyObject *attr,
        const char *attr_name,
        PyTypes::ObjectSharedPtr &member
    );

}
