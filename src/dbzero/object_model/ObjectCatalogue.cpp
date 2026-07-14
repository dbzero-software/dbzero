// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ObjectCatalogue.hpp"

namespace db0::object_model

{

    ObjectCatalogue::ObjectCatalogue(db0::Memspace &memspace)
        : super_t(memspace)
    {
    }

    ObjectCatalogue::ObjectCatalogue(db0::mptr ptr)
        : super_t(ptr)
    {
    }

}