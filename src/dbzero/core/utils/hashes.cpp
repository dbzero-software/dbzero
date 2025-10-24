#pragma once
#include "hashes.hpp"

namespace db0

{
    uint64_t murmurhash64A(const void* key, size_t len, uint64_t seed)
    {
        const uint64_t m = 0xc6a4a7935bd1e995ULL;
        const int r = 47;

        uint64_t h = seed ^ (len * m);

        const uint64_t* data = (const uint64_t*)key;
        const uint64_t* end = data + (len / 8);

        //----------
        // Body: process 8 bytes (64 bits) at a time
        while (data != end) {
            uint64_t k = *data++;

            k *= m;
            k ^= k >> r;
            k *= m;

            h ^= k;
            h *= m;
        }

        //----------
        // Tail: handle remaining bytes (less than 8)
        const unsigned char* tail = (const unsigned char*)data;

        switch (len & 7) {
            case 7: h ^= (uint64_t)tail[6] << 48;
            case 6: h ^= (uint64_t)tail[5] << 40;
            case 5: h ^= (uint64_t)tail[4] << 32;
            case 4: h ^= (uint64_t)tail[3] << 24;
            case 3: h ^= (uint64_t)tail[2] << 16;
            case 2: h ^= (uint64_t)tail[1] << 8;
            case 1: h ^= (uint64_t)tail[0];
                    h *= m;
        };

        //----------
        // Finalization mix
        h ^= h >> r;
        h *= m;
        h ^= h >> r;

        return h;
    }

}