#pragma once

#include <unordered_map>
#include <dbzero/core/exception/AbstractException.hpp>

namespace db0

{

    /// CloneMap lets you track source instance from final instance after clone operation
    template <typename T>
    class CloneMap : protected std::unordered_map<const T*, const T*> 
    {
    public :

        /**
         * Register mapping from -> to (as result of clone operation)
         * NOTICE: multiple mappings from same instance are acceptable
         * @param to final instance
         * @param from origina (source) instance
         */
        void insert(const T &to, const T &from) 
        {
            const T *from_ptr = &from;
            // try find "source" mapping (i.e. if A -> B exists and we register B -> C then A -> C will be the final registered mapping)
            for (;;) {
                auto it = this->find(from_ptr);
                if (it == this->end()) {
                    break;
                }
                from_ptr = it->second;
            }
            // register
            (*this)[&to] = from_ptr;
        }

        /**
         * Find source mapping or throw (db0::InputException)
         * @param to
         * @return source mapping
         */
        const T &findSource (const T &to) const 
        {
            auto it = this->find(&to);
            if (it==this->end()) {
                THROWF(db0::InputException) << "Source mapping is unknown";
            }
            return *it->second;
        }

        /**
         * Resolve "final" address from source
         * @param from
         * @return nullptr if resolution not possible
         */
        const T *tryFindFinal (const T &from) const 
        {
            for (auto &p: *this) {
                if (p.second==&from) {
                    return p.first;
                }
            }
            return nullptr;
        }

    };

}
