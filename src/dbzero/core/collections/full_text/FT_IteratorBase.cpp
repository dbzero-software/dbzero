#include "FT_IteratorBase.hpp"
#include "FT_Runnable.hpp"

namespace db0

{
    
    const FT_IteratorBase *FT_IteratorBase::find(const FT_IteratorBase &it) const
    {
        if (equal(it)) {
            return this;
        } else {
            return nullptr;
        }
    }
    
}