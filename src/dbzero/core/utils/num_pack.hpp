#pragma once

namespace db0

{

    // tuple/array of N-numbers of type T
    template <typename T, unsigned int N> struct [[gnu::packed]] num_pack
    {
        T data[N];

        num_pack() = default;
        num_pack(const num_pack &other) = default;        
        num_pack(num_pack &&other) noexcept = default;

        // construct from N numbers (N >= 2)
        template <typename... Args> num_pack(T first, T second, Args... args) : data{first, second, args...} {}
        
        num_pack &operator=(const num_pack &other) = default;
        
        num_pack &operator=(num_pack &&other) noexcept = default;

        bool operator<(const num_pack &other) const;

        bool operator==(const num_pack &other) const;

        bool operator!=(const num_pack &other) const;

        inline T operator[](unsigned int i) const {
            return data[i];
        }
    };
    
    template <typename T, unsigned int N> bool num_pack<T, N>::operator<(const num_pack &other) const
    {
        for (unsigned int i = 0; i < N; i++) {
            if (data[i] < other.data[i])
                return true;
            if (data[i] > other.data[i])
                return false;
        }
        return false;
    }

    template <typename T, unsigned int N> bool num_pack<T, N>::operator==(const num_pack &other) const
    {
        for (unsigned int i = 0; i < N; i++) {
            if (data[i] != other.data[i])
                return false;
        }
        return true;
    }

    template <typename T, unsigned int N> bool num_pack<T, N>::operator!=(const num_pack &other) const {
        return !(*this == other);
    }
        
}
