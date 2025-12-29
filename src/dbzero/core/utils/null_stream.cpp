// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "null_stream.hpp"

namespace db0::utils

{

	int NullBuffer::overflow(int c)
    {
        return c;
    }
    
    NullBuffer nullBuffer; 
    std::ostream nullStream(&nullBuffer);

}
