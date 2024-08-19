#pragma once
#include <mutex>

namespace db0::python
{
    extern std::mutex python_bindings_mutex;
} // namespace db0::python