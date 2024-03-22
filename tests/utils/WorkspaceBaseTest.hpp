#include <gtest/gtest.h>
#include "TestWorkspace.hpp"
#include <dbzero/core/memory/Memspace.hpp>
#include <dbzero/core/utils/null_stream.hpp>

namespace tests

{

	class WorkspaceBaseTest: public testing::Test
	{
	public :        

		WorkspaceBaseTest()
            : WorkspaceBaseTest(db0::utils::nullStream)
		{
		}

		WorkspaceBaseTest(std::ostream &log)
			: log(log)
		{
		}

        db0::Memspace getMemspace()
        {
            return m_workspace.getMemspace("my-test-prefix_1");
        }
		
		void TearDown() override
		{
			m_workspace.tearDown();
		}

	protected:
		std::ostream &log;
        db0::TestWorkspace m_workspace;
	};
    
}