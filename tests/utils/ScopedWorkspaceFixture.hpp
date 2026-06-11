// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 DBZero Software sp. z o.o.

#pragma once

#include <string>

#include <dbzero/core/memory/swine_ptr.hpp>
#include <dbzero/object_model/ObjectModel.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <utils/utils.hpp>

namespace tests
{

    class ScopedWorkspaceFixture
    {
    public:
        explicit ScopedWorkspaceFixture(const char *prefix_name)
            : m_prefix_name(prefix_name)
            , m_workspace("", {}, {}, {}, {}, db0::object_model::initializer())
        {
            db0::tests::dropPrefixFiles(m_prefix_name.c_str());
            m_fixture = m_workspace.getFixture(m_prefix_name);
        }

        ~ScopedWorkspaceFixture()
        {
            close();
            db0::tests::dropPrefixFiles(m_prefix_name.c_str());
        }

        ScopedWorkspaceFixture(const ScopedWorkspaceFixture &) = delete;
        ScopedWorkspaceFixture &operator=(const ScopedWorkspaceFixture &) = delete;

        db0::swine_ptr<db0::Fixture> &fixture()
        {
            return m_fixture;
        }

        db0::Workspace &workspace()
        {
            return m_workspace;
        }

        void close()
        {
            if (!m_closed) {
                m_fixture = nullptr;
                m_workspace.close();
                m_closed = true;
            }
        }

    private:
        std::string m_prefix_name;
        db0::Workspace m_workspace;
        db0::swine_ptr<db0::Fixture> m_fixture;
        bool m_closed = false;
    };

}
