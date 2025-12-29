// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <array>
#include <cstdint>
#include <vector>
#include <algorithm>

namespace db0

{

    /**
     * A sequence of binary streams of fixed size
    */
    template <std::size_t size> class BlobSequence
    {
    public:
        BlobSequence(std::vector<std::byte> &);
        BlobSequence(std::byte *begin, std::byte *end);

        struct Blob: public std::array<std::byte, size>
        {            
            bool operator<(const Blob &other) const;        
        };        

        // sort the sequence in place
        void sort();

    private:
        Blob *m_begin;
        Blob *m_end;
    };
    
    template <std::size_t size> BlobSequence<size>::BlobSequence(std::vector<std::byte> &bytes)
        : BlobSequence(bytes.data(), bytes.data() + bytes.size())
    {
    }

    template <std::size_t size> BlobSequence<size>::BlobSequence(std::byte *begin, std::byte *end)
    {
        if ((end - begin) % size != 0) {
            THROWF(db0::InternalException) << "Invalid size of the byte sequence";
        }
        m_begin = reinterpret_cast<Blob *>(begin);
        m_end = reinterpret_cast<Blob *>(end);
    }

    template <std::size_t size> bool BlobSequence<size>::Blob::operator<(const Blob &other) const
    {
        for (std::size_t i = 0; i < size; ++i) {
            if (this->at(i) < other.at(i)) {
                return true;
            } else if (this->at(i) > other.at(i)) {
                return false;
            }
        }
        return false;
    }
    
    template <std::size_t size> void BlobSequence<size>::sort() {
        std::sort(m_begin, m_end);
    }

}
