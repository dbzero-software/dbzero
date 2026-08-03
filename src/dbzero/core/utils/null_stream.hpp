// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#pragma once

#include <streambuf>
#include <ostream>

namespace db0::utils

{

	class NullBuffer : public std::streambuf
	{
	public:
  		int overflow(int c);
	};	
	
	extern NullBuffer nullBuffer;
	extern std::ostream nullStream;
	
}
