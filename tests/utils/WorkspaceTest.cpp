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
    
}
