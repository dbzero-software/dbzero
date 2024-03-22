#pragma once

#include <memory>

namespace db0

{

    /// Construct shared_ptr with deleter
    template <typename T, typename... Args> std::shared_ptr<void> make_shared_void (Args&&... args) {
        return std::shared_ptr<void>(new T(std::forward<Args>(args)...), [](T *ptr) {
            delete ptr;
        });
    }
    
} 
