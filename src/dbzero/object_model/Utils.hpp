// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/memory/Address.hpp>

namespace db0::object_model

{

    template <typename MemoT>
    UniqueAddress getMemoUniqueAddress(MemoT *memo_obj)
    {
        return memo_obj->ext().getUniqueAddress();
    }

}
