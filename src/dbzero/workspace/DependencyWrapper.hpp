#pragma once

#include <memory>

namespace db0

{

    /// Simple interface for sharing instance dependencies
    class DependencyHolder
    {
    public :
        DependencyHolder() = default;
        virtual ~DependencyHolder() = default;
    };

    template <typename T> class DependencyWrapper: public DependencyHolder
    {
        std::shared_ptr<const T> m_ptr;
    public :
        DependencyWrapper (std::shared_ptr<const T> ptr)
            : m_ptr(ptr)
        {
        }
    };
    
}
