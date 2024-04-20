#include "WorkspaceTest.hpp"

namespace tests

{

    db0::Memspace WorkspaceBaseTest::getMemspace() {
        return m_workspace.getMemspace("my-test-prefix_1");
    }
		
	void WorkspaceBaseTest::TearDown() {
		m_workspace.tearDown();
	}

    void WorkspaceTest::TearDown() {
		m_workspace.tearDown();
	}
	
	db0::swine_ptr<db0::Fixture> WorkspaceTest::getFixture() {
		return m_workspace.getFixture("test-fixture-1");
	}
	
}
