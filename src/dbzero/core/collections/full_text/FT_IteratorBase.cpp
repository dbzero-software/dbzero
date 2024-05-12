#include "FT_IteratorBase.hpp"
#include <dbzero/core/utils/blob_sort.hpp>

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
    
    bool FT_IteratorBase::isSimple() const {
        return false;
    }
    
    double FT_IteratorBase::compareTo(const FT_IteratorBase &it) const
    {
        if (this->isSimple() && !it.isSimple()) {
            // invert the comparison order (call over a non-simple iterator)
            return it.compareToImpl(*this);
        } else {
            return this->compareToImpl(it);
        }
    }

    void sortSignatures(std::vector<std::byte> &bytes) {
        db0::BlobSequence<FT_IteratorBase::SIGNATURE_SIZE>(bytes).sort();
    }

    void sortSignatures(std::byte *begin, std::byte *end) {
        db0::BlobSequence<FT_IteratorBase::SIGNATURE_SIZE>(begin, end).sort();
    }

}