// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "FT_IteratorBase.hpp"
#include <dbzero/core/utils/blob_sort.hpp>

namespace db0

{
    
    std::atomic<std::uint64_t> FT_IteratorBase::s_next_uid = 1;

    FT_IteratorBase::FT_IteratorBase()
        : m_uid(nextUID())
    {
    }
    
    const FT_IteratorBase *FT_IteratorBase::find(std::uint64_t uid) const
    {
        if (m_uid == uid) {
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
    
    std::uint64_t FT_IteratorBase::nextUID() {
        return s_next_uid++;
    }
    
    bool FT_IteratorBase::skip(std::size_t count)
    {        
        for (std::size_t i = 0; i < count; ++i) {
            if (isEnd()) {
                return false;
            }
            next();            
        }
        return true;
    }
    
}