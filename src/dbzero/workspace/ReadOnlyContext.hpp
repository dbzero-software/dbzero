// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <atomic>
#include <cstdint>

namespace db0

{

    class ReadOnlyContext
    {
    public:
        using DepthProvider = unsigned int (*)();

        ReadOnlyContext();
        ~ReadOnlyContext();

        void close();

        static bool isActive();
        static unsigned int depth();
        static void setDepthProvider(DepthProvider provider);
        static void enterExternal();
        static void exitExternal();

    private:
        bool m_active = true;
        static std::atomic_uint64_t s_total_depth;
        static thread_local unsigned int s_depth;
        static DepthProvider s_depth_provider;
    };

}
