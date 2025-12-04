// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "TestBase.hpp"

namespace tests

{

    db0::Memspace MemspaceTestBase::getMemspace() {
        return m_workspace.getMemspace("my-test-prefix_1");
    }
		
	void MemspaceTestBase::TearDown() {
		m_workspace.tearDown();
	}

    void FixtureTestBase::TearDown() {
		m_workspace.tearDown();
	}
	
	db0::swine_ptr<db0::Fixture> FixtureTestBase::getFixture() {
		return m_workspace.getFixture("test-fixture-1");
	}
	
}
