// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include "Fixture.hpp"

namespace db0

{

#ifndef NDEBUG

    std::uint64_t writeBytes(db0::swine_ptr<Fixture>, const char *data, std::size_t len);

    void freeBytes(db0::swine_ptr<Fixture>, Address);

    std::string readBytes(db0::swine_ptr<Fixture>, Address);

#endif

}