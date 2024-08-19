#include "dbzero/bindings/python/GlobalMutex.hpp"

namespace db0::python
{
    std::mutex python_bindings_mutex;
} // namespace db0::python