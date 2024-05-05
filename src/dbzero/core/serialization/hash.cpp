#include "hash.hpp"
#include <cstdint>

namespace db0::serial

{

    constexpr std::uint32_t ROTL32(std::uint32_t x, std::uint32_t n) {
        return (x << n) | (x >> (32 - n));
    }

    constexpr std::uint64_t ROTL64(std::uint64_t x, std::uint64_t n) {
        return (x << n) | (x >> (64 - n));
    }

    constexpr std::uint32_t CH(std::uint32_t x, std::uint32_t y, std::uint32_t z) {
        return (x & y) ^ (~x & z);
    }

    constexpr std::uint32_t MAJ(std::uint32_t x, std::uint32_t y, std::uint32_t z) {
        return (x & y) ^ (x & z) ^ (y & z);
    }

    constexpr std::uint32_t EP0(std::uint32_t x) {
        return ROTL32(x, 2) ^ ROTL32(x, 13) ^ ROTL32(x, 22);
    }

    constexpr std::uint32_t EP1(std::uint32_t x) {
        return ROTL32(x, 6) ^ ROTL32(x, 11) ^ ROTL32(x, 25);
    }

    constexpr std::uint32_t SIG0(std::uint32_t x) {
        return ROTL32(x, 7) ^ ROTL32(x, 18) ^ (x >> 3);
    }

    constexpr std::uint32_t SIG1(std::uint32_t x) {
        return ROTL32(x, 17) ^ ROTL32(x, 19) ^ (x >> 10);
    }

    void sha256(const std::uint8_t* message, std::uint64_t len, std::uint32_t hash[8])
    {
        std::uint32_t h0 = 0x6a09e667;
        std::uint32_t h1 = 0xbb67ae85;
        std::uint32_t h2 = 0x3c6ef372;
        std::uint32_t h3 = 0xa54ff53a;
        std::uint32_t h4 = 0x510e527f;
        std::uint32_t h5 = 0x9b05688c;
        std::uint32_t h6 = 0x1f83d9ab;
        std::uint32_t h7 = 0x5be0cd19;

        std::uint32_t w[64];
        for (int i = 0; i < 64; ++i) {
            if (i < 16) {
                w[i] = (message[i * 4] << 24) | (message[i * 4 + 1] << 16) | (message[i * 4 + 2] << 8) | (message[i * 4 + 3]);
            } else {
                w[i] = SIG1(w[i - 2]) + w[i - 7] + SIG0(w[i - 15]) + w[i - 16];
            }
        }

        std::uint32_t a, b, c, e, f, g, h;
        for (int i = 0; i < 64; ++i) {
            a = h0;
            b = h1;
            c = h2;            
            e = h4;
            f = h5;
            g = h6;
            h = h7;

            std::uint32_t t1 = h + EP1(e) + CH(e, f, g) + 0x428a2f98 + w[i];
            std::uint32_t t2 = EP0(a) + MAJ(a, b, c);

            h7 = h6;
            h6 = h5;
            h5 = h4;
            h4 = h3 + t1;
            h3 = h2;
            h2 = h1;
            h1 = h0;
            h0 = t1 + t2;
        }

        hash[0] = h0 + 0x6a09e667;
        hash[1] = h1 + 0xbb67ae85;
        hash[2] = h2 + 0x3c6ef372;
        hash[3] = h3 + 0xa54ff53a;
        hash[4] = h4 + 0x510e527f;
        hash[5] = h5 + 0x9b05688c;
        hash[6] = h6 + 0x1f83d9ab;
        hash[7] = h7 + 0x5be0cd19;
    }
    
}
