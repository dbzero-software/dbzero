// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

namespace db0

{

    struct RootSparsePairConfig;
    struct PlainSparsePairConfig;
    template <typename ConfigT> class SparsePairBase;

    using RootSparsePair = SparsePairBase<RootSparsePairConfig>;
    using PlainSparsePair = SparsePairBase<PlainSparsePairConfig>;
    using SparsePair = RootSparsePair;

}
