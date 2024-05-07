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

    std::unique_ptr<FT_Runnable> FT_IteratorBase::extractRunnable() const
    {
        FT_Runnable *at_ptr = reinterpret_cast<FT_Runnable*>(new char[sizeof(FT_Runnable)]);
        this->extractRunnable(at_ptr);
        return std::unique_ptr<FT_Runnable>(at_ptr);
    }

}