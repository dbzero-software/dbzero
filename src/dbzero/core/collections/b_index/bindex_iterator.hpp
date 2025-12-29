// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <unordered_set>
#include <dbzero/core/utils/shared_void.hpp>
#include "mb_index_generic_input_range.hpp"
#include "bindex_interface.hpp"

namespace db0::bindex::iterator 

{

    /**
     * Promote joinable const iterators to top level types (need this for proper partial type resolution)
     * T data collection type
     */
    template <typename AddrT> class empty_joinable_const
        : public db0::empty_index<AddrT>::const_iterator 
    {
    };
    
    template <typename item_t, typename AddrT, typename... T> class itty_joinable_const
        : public db0::IttyIndex<item_t, AddrT, T...>::joinable_const_iterator 
    {
    };

    template <typename item_t, int N, typename AddrT, typename... T> class array_joinable_const
        : public db0::v_sorted_sequence<item_t, N, AddrT, T...>::joinable_const_iterator 
    {
    };

    template <typename item_t, typename AddrT, typename... T> class sorted_vector_joinable_const
        : public db0::v_sorted_vector<item_t, AddrT, T...>::joinable_const_iterator 
    {
    };
    
    template <typename item_t, typename AddrT, typename... T> class bindex_joinable_const:
    public db0::v_bindex<item_t, AddrT, T...>::joinable_const_iterator {
    public :
        using super_t = typename db0::v_bindex<item_t, AddrT, T...>::joinable_const_iterator;
    };

    template <typename item_t> using incrementPtr = void (*)(void *this_ptr);
    template <typename item_t> using decrementPtr = void (*)(void *this_ptr);
    template <typename item_t> using constDerefPtr = const item_t &(*)(const void *this_ptr);
    template <typename item_t> using isEndPtr = bool (*)(const void *this_ptr);
    template <typename item_t> using stopPtr = void (*)(void *this_ptr);
    template <typename item_t> using joinPtr = bool (*)(void *this_ptr, const item_t &key, int direction);
    template <typename item_t> using joinBoundPtr = void (*)(void *this_ptr, const item_t &key);
    template <typename item_t> using peekPtr = std::pair<item_t, bool> (*)(const void *this_ptr, const item_t &key);        
    template <typename item_t> using limitByPtr = bool (*)(void *this_ptr, const item_t &);
    template <typename item_t> using hasLimitPtr = bool (*)(const void *this_ptr);
    template <typename item_t> using getLimitPtr = const item_t& (*)(const void *this_ptr);
    template <typename item_t> using clonePtr = std::shared_ptr<void> (*)(const void *this_ptr);
    template <typename item_t> using isNextKeyDuplicatedPtr = bool (*)(const void *this_ptr);

    template <typename item_t, typename T> struct IncrementFunctor {
    };

    template <typename item_t, typename T> struct DecrementFunctor {
    };

    template <typename item_t, typename T> struct ConstDerefFunctor {
    };

    template <typename item_t, typename T> struct IsEndFunctor {
    };

    template <typename item_t, typename T> struct StopFunctor {
    };

    template <typename item_t, typename T> struct JoinFunctor {
    };

    template <typename item_t, typename T> struct JoinBoundFunctor {
    };

    template <typename item_t, typename T> struct PeekFunctor {
    };

    template <typename item_t, typename T> struct LimitByFunctor {
    };

    template <typename item_t, typename T> struct HasLimitFunctor {
    };

    template <typename item_t, typename T> struct GetLimitFunctor {
    };

    template <typename item_t, typename T> struct CloneFunctor {
    };

    template <typename item_t, typename T> struct IsNextKeyDuplicatedFunctor {
    };

    /**
     * empty_index::const_iterator specializations
     */
    template <typename item_t, typename... T> struct IncrementFunctor<item_t, empty_joinable_const<T...> > {
        static void execute(void *) {
            assert(false);
            THROWF(db0::InternalException) << "increment out of bounds with empty const iterator";
        }
    };

    template <typename item_t, typename... T> struct DecrementFunctor<item_t, empty_joinable_const<T...> > {
        static void execute(void *) {
            assert(false);
            THROWF(db0::InternalException) << "increment out of bounds with empty const iterator";
        }
    };

    template <typename item_t, typename... T> struct ConstDerefFunctor<item_t, empty_joinable_const<T...> > {
        static const item_t &execute(const void *) {
            assert(false);
            THROWF(db0::InternalException) << "attempt to dereference end iterator" << THROWF_END;
        }
    };

    template <typename item_t, typename... T> struct IsEndFunctor<item_t, empty_joinable_const<T...> > {
        static bool execute(const void *) {
            return true;
        }
    };

    template <typename item_t, typename... T> struct StopFunctor<item_t, empty_joinable_const<T...> > {
        static void execute(void *) {
        }
    };

    template <typename item_t, typename... T> struct JoinFunctor<item_t, empty_joinable_const<T...> > {
        static bool execute(void *, const item_t &, int) {
            return false;
        }
    };

    template <typename item_t, typename... T> struct JoinBoundFunctor<item_t, empty_joinable_const<T...> > {
        static void execute(void *, const item_t &) {
        }
    };

    template <typename item_t, typename... T> struct PeekFunctor<item_t, empty_joinable_const<T...> > {
        static std::pair<item_t,bool> execute(const void *, const item_t &key) {
            return std::make_pair(key, false);
        }
    };
    
    template <typename item_t, typename... T> struct LimitByFunctor<item_t, empty_joinable_const<T...> > {
        static bool execute(void *, const item_t&) {
            return false;
        }
    };

    template <typename item_t, typename... T> struct HasLimitFunctor<item_t, empty_joinable_const<T...> > {
        static bool execute(const void *) {
            return false;
        }
    };

    template <typename item_t, typename... T> struct GetLimitFunctor<item_t, empty_joinable_const<T...> > {
        static const item_t& execute(const void *) {
            assert(false);
            item_t* result = nullptr;
            return *result;
        }
    };

    template <typename item_t, typename... T> struct CloneFunctor<item_t, empty_joinable_const<T...> > {
        static std::shared_ptr<void> execute(const void*) {
            return db0::make_shared_void<empty_joinable_const<T...> >();
        }
    };

    template <typename item_t, typename... T> struct IsNextKeyDuplicatedFunctor<item_t, empty_joinable_const<T...> > {
        static bool execute(const void *) {
            return false;
        }
    };

    /**
     * itty_index::const_iterator specializations
     */
    template <typename item_t, typename... T> struct IncrementFunctor<item_t, itty_joinable_const<item_t, T...> > {
        static void execute(void *this_ptr) {
            ++(*reinterpret_cast<itty_joinable_const<item_t, T...>*>(this_ptr));
        }
    };

    template <typename item_t, typename... T> struct DecrementFunctor<item_t, itty_joinable_const<item_t, T...> > {
        static void execute(void *this_ptr) {
            --(*reinterpret_cast<itty_joinable_const<item_t, T...>*>(this_ptr));
        }
    };

    template <typename item_t, typename... T> struct ConstDerefFunctor<item_t, itty_joinable_const<item_t, T...> > {
        static const item_t &execute(const void *this_ptr) {
            return **reinterpret_cast<const itty_joinable_const<item_t, T...>*>(this_ptr);
        }
    };

    template <typename item_t, typename... T> struct IsEndFunctor<item_t, itty_joinable_const<item_t, T...> > {
        static bool execute(const void *this_ptr) {
            return reinterpret_cast<const itty_joinable_const<item_t, T...>*>(this_ptr)->is_end();
        }
    };

    template <typename item_t, typename... T> struct StopFunctor<item_t, itty_joinable_const<item_t, T...> > {
        static void execute(void *this_ptr) {
            reinterpret_cast<itty_joinable_const<item_t, T...>*>(this_ptr)->stop();
        }
    };

    template <typename item_t, typename... T> struct JoinFunctor<item_t, itty_joinable_const<item_t, T...> > {
        static bool execute(void *this_ptr, const item_t &key, int direction) {
            auto  &it = *reinterpret_cast<itty_joinable_const<item_t, T...>*>(this_ptr);            
            return it.join(key, direction);
        }
    };

    template <typename item_t, typename... T> struct JoinBoundFunctor<item_t, itty_joinable_const<item_t, T...> > {
        static void execute(void *this_ptr, const item_t &key) {
            reinterpret_cast<itty_joinable_const<item_t, T...>*>(this_ptr)->joinBound(key);
        }
    };

    template <typename item_t, typename... T> struct PeekFunctor<item_t, itty_joinable_const<item_t, T...> > {
        static std::pair<item_t, bool> execute(const void *this_ptr, const item_t &key) {
            return reinterpret_cast<const itty_joinable_const<item_t, T...>*>(this_ptr)->peek(key);
        }
    };

    template <typename item_t, typename... T> struct LimitByFunctor<item_t, itty_joinable_const<item_t, T...> > {
        static bool execute(void *this_ptr, const item_t &key) {
            using self_t = itty_joinable_const<item_t, T...>;
            return reinterpret_cast<self_t*>(this_ptr)->limitBy(key);
        }
    };

    template <typename item_t, typename... T>
    struct HasLimitFunctor<item_t, itty_joinable_const<item_t, T...> > {
        static bool execute(const void * this_ptr) {
            using self_t = itty_joinable_const<item_t, T...>;
            return static_cast<const self_t*>(this_ptr)->hasLimit();
        }
    };

    template <typename item_t, typename... T>
    struct GetLimitFunctor<item_t, itty_joinable_const<item_t, T...> > {
        static const item_t& execute(const void * this_ptr) {
            using self_t = itty_joinable_const<item_t, T...>;
            return static_cast<const self_t*>(this_ptr)->getLimit();
        }
    };

    template <typename item_t, typename... T> struct CloneFunctor<item_t, itty_joinable_const<item_t, T...> > {
        static std::shared_ptr<void> execute(const void *this_ptr) {
            using self_t = itty_joinable_const<item_t, T...>;
            const self_t &it = *reinterpret_cast<const self_t*>(this_ptr);
            return db0::make_shared_void<self_t>(it);
        }
    };

    template <typename item_t, typename... T>
    struct IsNextKeyDuplicatedFunctor<item_t, itty_joinable_const<item_t, T...> > {
        static bool execute(const void *) {
            return false;
        }
    };

    /**
     * array_index::joinable_const_iterator specializations
     */
    template <typename item_t, int N, typename... T> struct IncrementFunctor<item_t, array_joinable_const<item_t, N, T...> > {
        static void execute(void *this_ptr) {
            ++(*reinterpret_cast<array_joinable_const<item_t, N, T...>*>(this_ptr));
        }
    };

    template <typename item_t, int N, typename... T> struct DecrementFunctor<item_t, array_joinable_const<item_t, N, T...> > {
        static void execute(void *this_ptr) {
            --(*reinterpret_cast<array_joinable_const<item_t, N, T...>*>(this_ptr));
        }
    };

    template <typename item_t, int N, typename... T> struct ConstDerefFunctor<item_t, array_joinable_const<item_t, N, T...> > {
        static const item_t &execute(const void *this_ptr) {
            return **reinterpret_cast<const array_joinable_const<item_t, N, T...>*>(this_ptr);
        }
    };

    template <typename item_t, int N, typename... T> struct IsEndFunctor<item_t, array_joinable_const<item_t, N, T...> > {
        static bool execute(const void *this_ptr) {
            return reinterpret_cast<const array_joinable_const<item_t, N, T...>*>(this_ptr)->is_end();
        }
    };

    template <typename item_t, int N, typename... T> struct StopFunctor<item_t, array_joinable_const<item_t, N, T...> > {
        static void execute(void *this_ptr) {
            return reinterpret_cast<array_joinable_const<item_t, N, T...>*>(this_ptr)->stop();
        }
    };

    template <typename item_t, int N, typename... T> struct JoinFunctor<item_t, array_joinable_const<item_t, N, T...> > {
        static bool execute(void *this_ptr, const item_t &key, int direction) {
            array_joinable_const<item_t, N, T...> &it =
                    *reinterpret_cast<array_joinable_const<item_t, N, T...>*>(this_ptr);            
            return it.join(key, direction);
        }
    };

    template <typename item_t, int N, typename... T> struct JoinBoundFunctor<item_t, array_joinable_const<item_t, N, T...> > {
        static void execute(void *this_ptr, const item_t &key) {
            return reinterpret_cast<array_joinable_const<item_t, N, T...>*>(this_ptr)->joinBound(key);
        }
    };

    template <typename item_t, int N, typename... T> struct PeekFunctor<item_t, array_joinable_const<item_t, N, T...> > {
        static std::pair<item_t, bool> execute(const void *this_ptr, const item_t &key) {
            return reinterpret_cast<const array_joinable_const<item_t, N, T...>*>(this_ptr)->peek(key);
        }
    };

    template <typename item_t, int N, typename... T> struct LimitByFunctor<item_t, array_joinable_const<item_t, N, T...> > {
        static bool execute(void *this_ptr, const item_t &key) {
            using self_t = array_joinable_const<item_t, N, T...>;
            self_t &it = *reinterpret_cast<self_t*>(this_ptr);
            return it.limitBy(key);
        }
    };

    template <typename item_t, int N, typename... T>
    struct HasLimitFunctor<item_t, array_joinable_const<item_t, N, T...> > {
        static bool execute(const void * this_ptr) {
            using self_t = array_joinable_const<item_t, N, T...>;
            const self_t &it = *static_cast<const self_t*>(this_ptr);
            return it.hasLimit();
        }
    };

    template <typename item_t, int N, typename... T>
    struct GetLimitFunctor<item_t, array_joinable_const<item_t, N, T...> > {
        static const item_t& execute(const void * this_ptr) {
            using self_t = array_joinable_const<item_t, N, T...>;
            const self_t &it = *static_cast<const self_t*>(this_ptr);
            return it.getLimit();
        }
    };

    template <typename item_t, int N, typename... T> struct CloneFunctor<item_t, array_joinable_const<item_t, N, T...> > {
        static std::shared_ptr<void> execute(const void *this_ptr) {
            using self_t = array_joinable_const<item_t, N, T...>;
            const self_t &it = *reinterpret_cast<const self_t*>(this_ptr);
            return db0::make_shared_void<self_t>(it);
        }
    };

    template <typename item_t, int N, typename... T>
    struct IsNextKeyDuplicatedFunctor<item_t, array_joinable_const<item_t, N, T...> > {
        static bool execute(const void * this_ptr) {
            using self_t = array_joinable_const<item_t, N, T...>;
            const self_t &it = *static_cast<const self_t*>(this_ptr);
            return it.isNextKeyDuplicated();
        }
    };
    
    /**
     * v_sorted_vector::joinable_const_iterator specializations
     */
    template <typename item_t, typename... T> struct IncrementFunctor<item_t, sorted_vector_joinable_const<item_t, T...> > {
        static void execute(void *this_ptr) {
            ++(*reinterpret_cast<sorted_vector_joinable_const<item_t, T...>*>(this_ptr));
        }
    };

    template <typename item_t, typename... T> struct DecrementFunctor<item_t, sorted_vector_joinable_const<item_t, T...> > {
        static void execute(void *this_ptr) {
            --(*reinterpret_cast<sorted_vector_joinable_const<item_t, T...>*>(this_ptr));
        }
    };

    template <typename item_t, typename... T> struct ConstDerefFunctor<item_t, sorted_vector_joinable_const<item_t, T...> > {
        static const item_t &execute(const void *this_ptr) {
            return **reinterpret_cast<const sorted_vector_joinable_const<item_t, T...>*>(this_ptr);
        }
    };

    template <typename item_t, typename... T> struct IsEndFunctor<item_t, sorted_vector_joinable_const<item_t, T...> > {
        static bool execute(const void *this_ptr) {
            return reinterpret_cast<const sorted_vector_joinable_const<item_t, T...>*>(this_ptr)->is_end();
        }
    };

    template <typename item_t, typename... T> struct StopFunctor<item_t, sorted_vector_joinable_const<item_t, T...> > {
        static void execute(void *this_ptr) {
            reinterpret_cast<sorted_vector_joinable_const<item_t, T...>*>(this_ptr)->stop();
        }
    };

    template <typename item_t, typename... T> struct JoinFunctor<item_t, sorted_vector_joinable_const<item_t, T...> > {
        static bool execute(void *this_ptr, const item_t &key, int direction) {
            sorted_vector_joinable_const<item_t, T...> &it =
                    *reinterpret_cast<sorted_vector_joinable_const<item_t, T...>*>(this_ptr);
            return it.join(key, direction);
        }
    };

    template <typename item_t, typename... T> struct JoinBoundFunctor<item_t, sorted_vector_joinable_const<item_t, T...> > {
        static void execute(void *this_ptr, const item_t &key) {
            reinterpret_cast<sorted_vector_joinable_const<item_t, T...>*>(this_ptr)->joinBound(key);
        }
    };

    template <typename item_t, typename... T> struct PeekFunctor<item_t, sorted_vector_joinable_const<item_t, T...> > {
        static std::pair<item_t, bool> execute(const void *this_ptr, const item_t &key) {
            return reinterpret_cast<const sorted_vector_joinable_const<item_t, T...>*>(this_ptr)->peek(key);
        }
    };

    template <typename item_t, typename... T> struct LimitByFunctor<item_t, sorted_vector_joinable_const<item_t, T...> > {
        static bool execute(void *this_ptr, const item_t &key) {
            using self_t = sorted_vector_joinable_const<item_t, T...>;
            self_t &it = *reinterpret_cast<self_t*>(this_ptr);
            return it.limitBy(key);
        }
    };

    template <typename item_t, typename... T>
    struct HasLimitFunctor<item_t, sorted_vector_joinable_const<item_t, T...> > {
        static bool execute(const void * this_ptr) {
            using self_t = sorted_vector_joinable_const<item_t, T...>;
            const self_t &it = *static_cast<const self_t*>(this_ptr);
            return it.hasLimit();
        }
    };

    template <typename item_t, typename... T>
    struct GetLimitFunctor<item_t, sorted_vector_joinable_const<item_t, T...> > {
        static const item_t& execute(const void * this_ptr) {
            using self_t = sorted_vector_joinable_const<item_t, T...>;
            const self_t &it = *static_cast<const self_t*>(this_ptr);
            return it.getLimit();
        }
    };

    template <typename item_t, typename... T> struct CloneFunctor<item_t, sorted_vector_joinable_const<item_t, T...> > {
        static std::shared_ptr<void> execute(const void *this_ptr) {
            using self_t = sorted_vector_joinable_const<item_t, T...>;
            const self_t &it = *reinterpret_cast<const self_t*>(this_ptr);
            return db0::make_shared_void<self_t>(it);
        }
    };

    template <typename item_t, typename... T>
    struct IsNextKeyDuplicatedFunctor<item_t, sorted_vector_joinable_const<item_t, T...> > {
        static bool execute(const void * this_ptr) {
            using self_t = sorted_vector_joinable_const<item_t, T...>;
            const self_t &it = *static_cast<const self_t*>(this_ptr);
            return it.isNextKeyDuplicated();
        }
    };

    /**
     * bindex::joinable_const_iterator specializations
     */
    template <typename item_t, typename... T> struct IncrementFunctor<item_t, bindex_joinable_const<item_t, T...> > {
        static void execute(void *this_ptr) {
            ++(*reinterpret_cast<bindex_joinable_const<item_t, T...>*>(this_ptr));
        }
    };

    template <typename item_t, typename... T> struct DecrementFunctor<item_t, bindex_joinable_const<item_t, T...> > {
        static void execute(void *this_ptr) {
            --(*reinterpret_cast<bindex_joinable_const<item_t, T...>*>(this_ptr));
        }
    };

    template <typename item_t, typename... T> struct ConstDerefFunctor<item_t, bindex_joinable_const<item_t, T...> > {
        static const item_t &execute(const void *this_ptr) {
            return **reinterpret_cast<const bindex_joinable_const<item_t, T...>*>(this_ptr);
        }
    };

    template <typename item_t, typename... T> struct IsEndFunctor<item_t, bindex_joinable_const<item_t, T...> > {
        static bool execute(const void *this_ptr) {
            return reinterpret_cast<const bindex_joinable_const<item_t, T...>*>(this_ptr)->is_end();
        }
    };

    template <typename item_t, typename... T> struct StopFunctor<item_t, bindex_joinable_const<item_t, T...> > {
        static void execute(void *this_ptr) {
            return reinterpret_cast<bindex_joinable_const<item_t, T...>*>(this_ptr)->stop();
        }
    };

    template <typename item_t, typename... T> struct JoinFunctor<item_t, bindex_joinable_const<item_t, T...> > {
        static bool execute(void *this_ptr, const item_t &key, int direction) {
            bindex_joinable_const<item_t, T...> &it =
                    *reinterpret_cast<bindex_joinable_const<item_t, T...>*>(this_ptr);            
            return it.join(key, direction);
        }
    };

    template <typename item_t, typename... T> struct JoinBoundFunctor<item_t, bindex_joinable_const<item_t, T...> > {
        static void execute(void *this_ptr, const item_t &key) {
            return reinterpret_cast<bindex_joinable_const<item_t, T...>*>(this_ptr)->joinBound(key);
        }
    };

    template <typename item_t, typename... T> struct PeekFunctor<item_t, bindex_joinable_const<item_t, T...> > {
        static std::pair<item_t, bool> execute(const void *this_ptr, const item_t &key) {
            return reinterpret_cast<const bindex_joinable_const<item_t, T...>*>(this_ptr)->peek(key);
        }
    };

    template <typename item_t, typename... T> struct LimitByFunctor<item_t, bindex_joinable_const<item_t, T...> > {
        static bool execute(void *this_ptr, const item_t &key) {
            using self_t = typename bindex_joinable_const<item_t, T...>::super_t;
            self_t &it = *reinterpret_cast<self_t*>(this_ptr);
            return it.limitBy(key);
        }
    };

    template <typename item_t, typename... T>
    struct HasLimitFunctor<item_t, bindex_joinable_const<item_t, T...> > {
        static bool execute(const void * this_ptr) {
            using self_t = bindex_joinable_const<item_t, T...>;
            const self_t &it = *static_cast<const self_t*>(this_ptr);
            return it.hasLimit();
        }
    };

    template <typename item_t, typename... T>
    struct GetLimitFunctor<item_t, bindex_joinable_const<item_t, T...> > {
        static const item_t& execute(const void * this_ptr) {
            using self_t = bindex_joinable_const<item_t, T...>;
            const self_t &it = *static_cast<const self_t*>(this_ptr);
            return it.getLimit();
        }
    };

    template <typename item_t, typename... T> struct CloneFunctor<item_t, bindex_joinable_const<item_t, T...> > {
        static std::shared_ptr<void> execute(const void *this_ptr) {
            using self_t = typename bindex_joinable_const<item_t, T...>::super_t;
            const self_t &it = *reinterpret_cast<const self_t*>(this_ptr);
            return db0::make_shared_void<self_t>(it);
        }
    };

    template <typename item_t, typename... T>
    struct IsNextKeyDuplicatedFunctor<item_t, bindex_joinable_const<item_t, T...> > {
        static bool execute(const void * this_ptr) {
            using self_t = bindex_joinable_const<item_t, T...>;
            const self_t &it = *static_cast<const self_t*>(this_ptr);
            return it.isNextKeyDuplicated();
        }
    };

    template <typename item_t> struct ImplFunctions {
        incrementPtr<item_t> m_increment_ptr;
        decrementPtr<item_t> m_decrement_ptr;
        constDerefPtr<item_t> m_const_deref_ptr;
        isEndPtr<item_t> m_is_end_ptr;
        stopPtr<item_t> m_stop;
        joinPtr<item_t> m_join_ptr;
        joinBoundPtr<item_t> m_join_bound_ptr;
        peekPtr<item_t> m_peek_ptr;
        limitByPtr<item_t> m_limit_by_ptr;
        hasLimitPtr<item_t> m_has_limit_ptr;
        getLimitPtr<item_t> m_get_limit_ptr;
        clonePtr<item_t> m_clone_ptr;
        isNextKeyDuplicatedPtr<item_t> m_is_next_key_duplicated_ptr;
    };

    /**
     * Implementation of common v_bindex const_iterator interface
     * iterator is dependent (references) actual collection instance (v_bindex)
     */
    template <typename item_t> class Impl
    {
    public :
        using self_t = Impl<item_t>;

        Impl() = default;

        /**
         * T is collection iterator type (not collection)
         * @param impl actual iterator instance
         * @param ref passed to hold ownership (persistence) and points to "impl"
         */
        template <typename T> Impl(const T*, std::shared_ptr<void> ref)
            : m_ref(ref)
            , m_ptr(m_ref.get())
            , m_functions { 
                IncrementFunctor<item_t, T>::execute,
                DecrementFunctor<item_t, T>::execute,
                ConstDerefFunctor<item_t, T>::execute,
                IsEndFunctor<item_t, T>::execute,
                StopFunctor<item_t, T>::execute,
                JoinFunctor<item_t, T>::execute,
                JoinBoundFunctor<item_t, T>::execute,
                PeekFunctor<item_t, T>::execute,
                LimitByFunctor<item_t, T>::execute,
                HasLimitFunctor<item_t, T>::execute,
                GetLimitFunctor<item_t, T>::execute,
                CloneFunctor<item_t, T>::execute,
                IsNextKeyDuplicatedFunctor<item_t, T>::execute
            }
        {
        }

        /**
         * Constructs Impl of the same type using other instance (ref)
         * @param ref instance reference
         */
        Impl(const self_t &self,std::shared_ptr<void> ref)
            : m_ref(ref)
            , m_ptr(m_ref.get())
            , m_functions(self.m_functions)
        {
        }

        Impl(const self_t &other)
            : m_ref(other.m_ref)
            , m_ptr(other.m_ptr)
            , m_functions(other.m_functions)
        {
        }

        Impl(self_t &&other)
            : m_ref(std::move(other.m_ref))
            , m_ptr(other.m_ptr)
            , m_functions(other.m_functions)
        {
            other.m_ptr = nullptr;
        }

        Impl &operator=(const self_t &other) {
            m_ref = std::move(other.m_ref);
            m_ptr = other.m_ptr;
            m_functions = other.m_functions;
            return *this;
        }

        Impl &operator=(self_t &&other) {
            m_ref = std::move(other.m_ref);
            m_ptr = other.m_ptr;
            m_functions = other.m_functions;

            other.m_ptr = nullptr;
            return *this;
        }

        void operator++() {
            m_functions.m_increment_ptr(m_ptr);
        }

        void operator--() {
            m_functions.m_decrement_ptr(m_ptr);
        }

        const item_t &operator*() const {
            return m_functions.m_const_deref_ptr(m_ptr);
        }

        bool is_end() const {
            return m_functions.m_is_end_ptr(m_ptr);
        }

        /**
         * join iterator forward or backward
         * @return end flag
         */
        bool join(const item_t &key, int direction) {
            return m_functions.m_join_ptr(m_ptr, key, direction);
        }

        void joinBound(const item_t &key) {
            m_functions.m_join_bound_ptr(m_ptr, key);
        }

        std::pair<item_t, bool> peek(const item_t &key) const {
            return m_functions.m_peek_ptr(m_ptr, key);
        }
        
        bool limitBy(const item_t &key) {
            return m_functions.m_limit_by_ptr(m_ptr, key);
        }

        bool hasLimit() const {
            return m_functions.m_has_limit_ptr(m_ptr);
        }

        const item_t &getLimit() const {
            return m_functions.m_get_limit_ptr(m_ptr);
        }

        /**
         * Clone performing deep copy
         * NOTICE: regular copy constructor only performs reference copy (shared instance)
         */
        self_t clone() const {
            return self_t(*this, m_functions.m_clone_ptr(m_ptr));
        }

        void stop() {
            m_functions.m_stop(m_ptr);
        }

        void reset() 
        {
            m_ref = nullptr;
            m_ptr = nullptr;
        }
        
        bool isNextKeyDuplicated() const {
            return m_functions.m_is_next_key_duplicated_ptr(m_ptr);
        }

    private:
        std::shared_ptr<void> m_ref;
        void *m_ptr = nullptr;
        ImplFunctions<item_t> m_functions;
    };

} 
