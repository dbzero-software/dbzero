#include "FT_Runnable.hpp"
#include "FT_IteratorBase.hpp"

namespace db0

{

    std::unique_ptr<FT_IteratorBase> FT_NullRunnable::run(db0::Snapshot &) const {
        throw std::runtime_error("FT_NullRunnable::run() not implemented");
    }
    
    FTRunnableType FT_NullRunnable::typeId() const {
        return FTRunnableType::Null;
    }
    
    void FT_NullRunnable::serialize(std::vector<std::byte> &) const
    {
    }

}
