// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#pragma once

#include "FT_Iterator.hpp"

namespace db0

{

    template <typename KeyT, typename KeyStorageT = KeyT>
    class FT_IteratorFactory
    {
    public:
        virtual ~FT_IteratorFactory() = default;
        
        virtual void add(std::unique_ptr<FT_Iterator<KeyT, KeyStorageT> > &&) = 0;
        
        virtual std::unique_ptr<FT_Iterator<KeyT, KeyStorageT> >
        release(int direction, bool lazy_init = false) = 0;

        // Invalidate / render empty
        virtual void clear() = 0;
    };
    
}   