// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ReadOnlyContext.hpp"
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{

    std::atomic_uint64_t ReadOnlyContext::s_total_depth = 0;
    thread_local unsigned int ReadOnlyContext::s_depth = 0;
    ReadOnlyContext::DepthProvider ReadOnlyContext::s_depth_provider = nullptr;

    ReadOnlyContext::ReadOnlyContext()
    {
        ++s_depth;
        enterExternal();
    }

    ReadOnlyContext::~ReadOnlyContext()
    {
        close();
    }

    void ReadOnlyContext::close()
    {
        if (!m_active) {
            return;
        }
        if (s_depth == 0) {
            THROWF(db0::InternalException) << "read_only context depth underflow" << THROWF_END;
        }
        --s_depth;
        exitExternal();
        m_active = false;
    }

    bool ReadOnlyContext::isActive()
    {
        if (s_total_depth.load(std::memory_order_relaxed) == 0) {
            return false;
        }
        if (s_depth > 0) {
            return true;
        }
        if (s_depth_provider) {
            return s_depth_provider() > 0;
        }
        return false;
    }

    unsigned int ReadOnlyContext::depth()
    {
        if (s_total_depth.load(std::memory_order_relaxed) == 0) {
            return 0;
        }
        auto result = s_depth;
        if (s_depth_provider) {
            result += s_depth_provider();
        }
        return result;
    }

    void ReadOnlyContext::setDepthProvider(DepthProvider provider)
    {
        s_depth_provider = provider;
    }

    void ReadOnlyContext::enterExternal()
    {
        s_total_depth.fetch_add(1, std::memory_order_relaxed);
    }

    void ReadOnlyContext::exitExternal()
    {
        auto previous_depth = s_total_depth.fetch_sub(1, std::memory_order_relaxed);
        if (previous_depth == 0) {
            s_total_depth.fetch_add(1, std::memory_order_relaxed);
            THROWF(db0::InternalException) << "read_only total depth underflow" << THROWF_END;
        }
    }

}
