#include <gtest/gtest.h>
#include "TestWorkspace.hpp"
#include <dbzero/core/memory/Memspace.hpp>
#include <dbzero/core/utils/null_stream.hpp>

namespace tests

{

	class WorkspaceBaseTest: public testing::Test
	{
	public:
		WorkspaceBaseTest()
            : WorkspaceBaseTest(db0::utils::nullStream)
		{
		}

		WorkspaceBaseTest(std::ostream &log)
			: log(log)
		{
		}

        db0::Memspace getMemspace();

		void TearDown() override;

	protected:
		std::ostream &log;
        db0::TestWorkspaceBase m_workspace;
	};

	class WorkspaceTest: public testing::Test
	{
	public:
		WorkspaceTest()
            : WorkspaceTest(db0::utils::nullStream)
		{
		}

		WorkspaceTest(std::ostream &log)
			: log(log)
		{
		}

		db0::swine_ptr<db0::Fixture> getFixture();

		void TearDown() override;
		
	protected:
		std::ostream &log;
        db0::TestWorkspace m_workspace;
	};
	
}