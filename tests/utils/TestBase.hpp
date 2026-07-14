// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include "TestWorkspace.hpp"
#include <dbzero/core/memory/Memspace.hpp>
#include <dbzero/core/utils/null_stream.hpp>

namespace tests

{

	class MemspaceTestBase: public testing::Test
	{
	public:
		MemspaceTestBase()
            : MemspaceTestBase(db0::utils::nullStream)
		{
		}

		MemspaceTestBase(std::ostream &log)
			: log(log)
		{
		}

        db0::Memspace getMemspace();

		void TearDown() override;

	protected:
		std::ostream &log;
        db0::TestWorkspaceBase m_workspace;
	};

	class FixtureTestBase: public testing::Test
	{
	public:
		FixtureTestBase()
            : FixtureTestBase(db0::utils::nullStream)
		{
		}

		FixtureTestBase(std::ostream &log)
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