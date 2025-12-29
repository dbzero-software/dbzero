// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <unordered_set>
#include <dbzero/core/utils/shared_void.hpp>
#include "mb_index_generic_input_range.hpp"
    
// This file contains specialized interface implementations for v_bindex compliant data structures

namespace db0::bindex

{

	std::ostream &operator<<(std::ostream &os, type);

}

namespace db0::bindex::interface

{

    /**
     * @return items requested / items actually inserted
     */
    template<typename DefinitionT>
    using bulkInsertUniquePtr = std::pair<std::uint32_t, std::uint32_t>(*)
        (void *this_ptr, typename DefinitionT::Containers::IInputRange&, std::function<void(typename DefinitionT::item_t)> *callback_ptr);

    template <typename item_t> using insertPtr = void (*)(void *this_ptr, const item_t &item);
    
    template<typename DefinitionT>
    using bulkErasePtr = std::size_t(*)
        (void *this_ptr, typename DefinitionT::Containers::IInputRange&, std::function<void(typename DefinitionT::item_t)> *callback_ptr);

    template <typename item_t> using erasePtr = void (*)(void *this_ptr, const item_t &);

    /**
     * @return true if collection is empty
     */
    template <typename item_t> using emptyPtr = bool (*)(const void *this_ptr);

    /**
     * @param direction required to initialize either for forwards or backwards iteration
     * @return collection specific const_iterator (as generic)
     */
    template <typename item_t> using beginPtr = std::shared_ptr<void> (*)(const void *this_ptr, int direction);

    /**
     * Destroys existing v-space instance
     */
    template <typename item_t> using destroyPtr = void (*)(void *this_ptr, Memspace &);

    /**
     * @return calculated size of collection (slow for v_bindex implementation)
     */
    template <typename item_t> using sizePtr = std::uint64_t (*)(const void *this_ptr);
    template <typename item_t> using sizeOfPtr = std::uint64_t (*)(const void *this_ptr);
    
    /**
     * Read all contents from data collection and store under "where"
     * @return number of elements copied
     */
    template <typename item_t> using copyAllPtr = std::size_t (*)(const void *this_ptr, item_t *where);

    /**
     * @param max_count bound to not exceed (since collection may be large it makes no sense to iterate past this mark)
     * @return number of unique (not present in current collection) elements \
     * passed in the input collection specified by begin / end iterators
     */
    template<typename DefinitionT>
    using countUniquePtr =
        std::size_t(*)(const void *this_ptr, typename DefinitionT::Containers::IInputRange&, std::size_t max_count);

    template <typename item_t> using findExistingPtr = bool (*)(const void *this_ptr, const item_t &);
    
    /**
     * Update existing element without affecting its key part
     * @return false if element was not found
    */
    template <typename item_t> using updateExistingPtr = bool (*)(void *this_ptr, const item_t &, item_t *);

    template <typename item_t> using findOnePtr = bool (*)(const void *this_ptr, item_t &);
    
    /**
     * @param max_count limit at which we stop counting
     * @return number of items existing in data collection for the ones requested to erase (hit count)
     */
    template<typename DefinitionT>
    using countExistingPtr =
        std::size_t(*)(const void *this_ptr, typename DefinitionT::Containers::IInputRange&, std::size_t max_count);

    template <typename item_t, typename AddrT> using getAddrPtr = AddrT (*)(const void *this_ptr);
    
    template <typename item_t> using commitPtr = void (*)(void *this_ptr);

    template <typename DefinitionT, typename T> struct BulkInsertUniqueFunctor {};

    template <typename item_t, typename T> struct InsertFunctor {};

    template <typename DefinitionT, typename T> struct BulkEraseFunctor {};

    template <typename item_t, typename T> struct EraseFunctor {};

    template <typename item_t, typename T> struct EmptyFunctor {};

    template <typename DefinitionT, typename T> struct CountUniqueFunctor {};

    template <typename item_t, typename T> struct FindExistingFunctor {};

    template <typename item_t, typename T> struct UpdateExistingFunctor {};

    template <typename item_t, typename T> struct FindOneFunctor {};

    template <typename DefinitionT, typename T> struct CountExistingFunctor {};

    template <typename item_t, typename T> struct SizeFunctor {};

    template <typename item_t, typename T> struct CopyAllFunctor {};

    template <typename item_t, typename T> struct SizeOfFunctor {};

    template <typename item_t, typename T> struct DestroyFunctor {};

    template <typename item_t, typename T> struct BeginFunctor {};

    template <typename item_t, typename T> struct GetAddrFunctor {};
    
    template <typename item_t, typename T> struct CommitFunctor {};

    template<typename DefinitionT, typename ContainerT>
    std::pair<std::uint32_t, std::uint32_t>
    insertGenericImpl(void *self, typename DefinitionT::Containers::IInputRange &input,
        std::function<void(typename DefinitionT::item_t)> *callback_ptr)
    {
        ContainerT &index = *reinterpret_cast<ContainerT*>(self);
        using InputRangeT = typename DefinitionT::template IContainerInputRange<ContainerT>;
        return static_cast<InputRangeT&>(input).insert(index, callback_ptr);
    }

    template<typename DefinitionT, typename ContainerT>
    std::size_t eraseGenericImpl(void *self, typename DefinitionT::Containers::IInputRange &input,
        std::function<void(typename DefinitionT::item_t)> *callback_ptr)
    {
        ContainerT &index = *reinterpret_cast<ContainerT*>(self);
        using InputRangeT = typename DefinitionT::template IContainerInputRange<ContainerT>;
        return static_cast<InputRangeT&>(input).erase(index, callback_ptr);
    }
    
    template<typename DefinitionT, typename ContainerT>
    std::size_t countNewGenericImpl(
        const void *self, typename DefinitionT::Containers::IInputRange &input, std::size_t max_count)
    {
        const ContainerT &index = *reinterpret_cast<const ContainerT*>(self);
        using InputRangeT = typename DefinitionT::template IContainerInputRange<ContainerT>;
        return static_cast<InputRangeT&>(input).countNew(index, max_count);
    }

    template<typename DefinitionT, typename ContainerT>
    std::size_t countExistingGenericImpl(
        const void *self, typename DefinitionT::Containers::IInputRange &input, std::size_t max_count)
    {
        const ContainerT &index = *reinterpret_cast<const ContainerT*>(self);
        using InputRangeT = typename DefinitionT::template IContainerInputRange<ContainerT>;
        return static_cast<InputRangeT&>(input).countExisting(index, max_count);
    }

    /**
     * v_bindex specializations
     */
    template<typename DefinitionT, typename... T>
    struct BulkInsertUniqueFunctor<DefinitionT, db0::v_bindex<T...>> {
        using CallbackT = std::function<void(typename DefinitionT::item_t)>;
        static std::pair<std::uint32_t, std::uint32_t>
        execute(void *this_ptr, typename DefinitionT::Containers::IInputRange &input, CallbackT *callback_ptr)
        {
            return insertGenericImpl<DefinitionT, db0::v_bindex<T...>>(this_ptr, input, callback_ptr);
        }
    };

    template <typename item_t, typename... T> struct InsertFunctor<item_t, db0::v_bindex<T...> > {
        static void execute(void *this_ptr, const item_t &item) {
            reinterpret_cast<db0::v_bindex<T...>*>(this_ptr)->insert(item);
        }
    };

    template<typename DefinitionT, typename... T>
    struct BulkEraseFunctor<DefinitionT, db0::v_bindex<T...>> {
        using CallbackT = std::function<void(typename DefinitionT::item_t)>;
        static std::size_t execute(void *this_ptr, typename DefinitionT::Containers::IInputRange &input, CallbackT *callback_ptr)
        {
            return eraseGenericImpl<DefinitionT, db0::v_bindex<T...>>(this_ptr, input, callback_ptr);
        }
    };

    template <typename item_t, typename... T> struct EraseFunctor<item_t, db0::v_bindex<T...> > {
        static void execute(void *this_ptr, const item_t &item) {
            db0::v_bindex<T...> &index = *reinterpret_cast<db0::v_bindex<T...>*>(this_ptr);
            auto it = index.find(item);
            if (it!=index.end()) {
                return index.erase(it);
            }
        }
    };

    template <typename item_t, typename... T> struct EmptyFunctor<item_t, db0::v_bindex<T...> > {
        static bool execute(const void *this_ptr) {
            const db0::v_bindex<T...> &index = *reinterpret_cast<const db0::v_bindex<T...>*>(this_ptr);
            return index.empty();
        }
    };

    template<typename DefinitionT, typename... T>
    struct CountUniqueFunctor<DefinitionT, db0::v_bindex<T...>> {
        static std::size_t execute(
            const void *this_ptr, typename DefinitionT::Containers::IInputRange &input, std::size_t max_count
        )
        {
            return countNewGenericImpl<DefinitionT, db0::v_bindex<T...>>(this_ptr, input, max_count);
        }
    };

    template <typename item_t, typename... T> struct FindExistingFunctor<item_t, db0::v_bindex<T...> > 
    {
        static bool execute(const void *this_ptr, const item_t &item) {
            const db0::v_bindex<T...> &index = *reinterpret_cast<const db0::v_bindex<T...>*>(this_ptr);
            return index.find(item)!=index.end();
        }
    };

    template <typename item_t, typename... T> struct UpdateExistingFunctor<item_t, db0::v_bindex<T...> >
    {
        static bool execute(void *this_ptr, const item_t &item, item_t *old_item) {
            db0::v_bindex<T...> &index = *reinterpret_cast<db0::v_bindex<T...>*>(this_ptr);
            return index.updateExisting(item, old_item);
        }
    };

    template <typename item_t, typename... T> struct FindOneFunctor<item_t, db0::v_bindex<T...> >
    {
        static bool execute(const void *this_ptr, item_t &item) {
            const db0::v_bindex<T...> &index = *reinterpret_cast<const db0::v_bindex<T...>*>(this_ptr);
            return index.findOne(item);
        }
    };

    template<typename DefinitionT, typename... T>
    struct CountExistingFunctor<DefinitionT, db0::v_bindex<T...> >
    {
        static std::size_t execute(
            const void *this_ptr, typename DefinitionT::Containers::IInputRange &input, std::size_t max_count
        )
        {
            return countExistingGenericImpl<DefinitionT, db0::v_bindex<T...>>(this_ptr, input, max_count);
        }
    };

    template <typename item_t, typename... T> struct SizeFunctor<item_t, db0::v_bindex<T...> > 
    {
        static std::uint64_t execute(const void *this_ptr) {
            const db0::v_bindex<T...> &index = *reinterpret_cast<const db0::v_bindex<T...>*>(this_ptr);
            return index.size();
        }
    };

    template <typename item_t, typename... T> struct GetAddrFunctor<item_t, db0::v_bindex<T...> > 
    {
        using AddrT = typename v_bindex<T...>::addr_t;
        static AddrT execute(const void *this_ptr) {
            const db0::v_bindex<T...> &index = *reinterpret_cast<const db0::v_bindex<T...>*>(this_ptr);
            return index.getAddress();
        }
    };

    template <typename item_t, typename... T> struct BeginFunctor<item_t, db0::v_bindex<T...> > {
        static std::shared_ptr<void> execute(const void *this_ptr, int direction) 
    {
            const db0::v_bindex<T...> &index = *reinterpret_cast<const db0::v_bindex<T...>*>(this_ptr);
            auto it = index.beginJoin(direction);
            using type_of_result = decltype(it);
            return db0::make_shared_void<decltype(it)>(it);
        }
    };

    template <typename item_t, typename... T> struct SizeOfFunctor<item_t, db0::v_bindex<T...> > {
        static std::uint64_t execute(const void *this_ptr) {
            const db0::v_bindex<T...> &index = *reinterpret_cast<const db0::v_bindex<T...>*>(this_ptr);
            return index.calculateStorageSize();
        }
    };

    template <typename item_t, typename... T> struct DestroyFunctor<item_t, db0::v_bindex<T...> > {
        static void execute(void *this_ptr, Memspace &memspace) {
            db0::v_bindex<T...> &index = *reinterpret_cast<db0::v_bindex<T...>*>(this_ptr);
            index.destroy();
        }
    };

    template <typename item_t, typename... T> struct CopyAllFunctor<item_t, db0::v_bindex<T...> > {
        static size_t execute(const void *this_ptr, item_t *where) {
            const db0::v_bindex<T...> &index = *reinterpret_cast<const db0::v_bindex<T...>*>(this_ptr);
            auto it = index.begin(), end = index.end();
            item_t *out = where;
            while (it!=end) {
                *out = *it;
                ++it;
                ++out;
            }
            return (out - where);
        }
    };

    template <typename item_t, typename... T> struct CommitFunctor<item_t, db0::v_bindex<T...> > 
    {
        static void execute(void *this_ptr) {
            db0::v_bindex<T...> &index = *reinterpret_cast<db0::v_bindex<T...>*>(this_ptr);            
            index.commit();
        }
    };

    /**
     * v_sorted_sequence specializations
     */
    template<typename DefinitionT, int N, typename... T> 
    struct BulkInsertUniqueFunctor<DefinitionT, db0::v_sorted_sequence<typename DefinitionT::item_t, N, T...>>
    {
        using CallbackT = std::function<void(typename DefinitionT::item_t)>;
        static std::pair<std::uint32_t, std::uint32_t>
        execute(void *this_ptr, typename DefinitionT::Containers::IInputRange &input, CallbackT *callback_ptr)
        {
            using ContainerT = db0::v_sorted_sequence<typename DefinitionT::item_t, N, T...>;
            return insertGenericImpl<DefinitionT, ContainerT>(this_ptr, input, callback_ptr);
        }
    };
    
    template <typename item_t, int N, typename... T> struct InsertFunctor<item_t, db0::v_sorted_sequence<item_t, N, T...> >
    {
        static void execute(void *, const item_t &) {
            THROWF(db0::InternalException) << "Cannot insert to immutable collection";
        }
    };

    template<typename DefinitionT, int N, typename... T>
    struct BulkEraseFunctor<DefinitionT, db0::v_sorted_sequence<typename DefinitionT::item_t, N, T...> > 
    {
        using CallbackT = std::function<void(typename DefinitionT::item_t)>;
        static std::size_t execute(void *this_ptr, typename DefinitionT::Containers::IInputRange &input, CallbackT *callback_ptr)
        {
            using ContainerT = db0::v_sorted_sequence<typename DefinitionT::item_t, N, T...>;
            return eraseGenericImpl<DefinitionT, ContainerT>(this_ptr, input, callback_ptr);
        }
    };

    template <typename item_t, int N, typename... T> struct EraseFunctor<item_t, db0::v_sorted_sequence<item_t, N, T...> > 
    {
        static void execute(void *, const item_t &) {
            THROWF(db0::InternalException)
                << "Operation not permitted, v_sorted_sequence is immutable collection" << THROWF_END;
        }
    };

    template <typename item_t, int N, typename... T> struct EmptyFunctor<item_t, db0::v_sorted_sequence<item_t, N, T...> > 
    {
        static bool execute(const void *) {
            return false;
        }
    };

    template <typename item_t, int N, typename... T> struct SizeFunctor<item_t, db0::v_sorted_sequence<item_t, N, T...> > 
    {
        static std::uint64_t execute(const void *) {
            return N;
        }
    };
    
    template <typename item_t, int N, typename... T> struct GetAddrFunctor<item_t, db0::v_sorted_sequence<item_t, N, T...> >
    {
        using SequenceT = db0::v_sorted_sequence<item_t, N, T...>;
        using AddrT = typename SequenceT::addr_t;
        static AddrT execute(const void *this_ptr) {
            const SequenceT &index = *reinterpret_cast<const SequenceT*>(this_ptr);
            return index.getAddress();
        }
    };

    template <typename item_t, int N, typename... T> struct BeginFunctor<item_t, db0::v_sorted_sequence<item_t, N, T...> > {
        static std::shared_ptr<void> execute(const void *this_ptr, int direction) 
    {
            const db0::v_sorted_sequence<item_t, N, T...> &index =
                    *reinterpret_cast<const db0::v_sorted_sequence<item_t, N, T...>*>(this_ptr);
            auto it = index.beginJoin(direction);
            return db0::make_shared_void<decltype(it)>(it);
        }
    };

    template <typename item_t, int N, typename... T> struct SizeOfFunctor<item_t, db0::v_sorted_sequence<item_t, N, T...> > {
        static std::uint64_t execute(const void *) {
            return db0::v_sorted_sequence<item_t, N, T...>::getStorageSize();
        }
    };

    template <typename item_t, int N, typename... T> struct DestroyFunctor<item_t, db0::v_sorted_sequence<item_t, N, T...> > {
        static void execute(void *this_ptr, Memspace &memspace) {
            db0::v_sorted_sequence<item_t, N, T...> &index =
                    *reinterpret_cast<db0::v_sorted_sequence<item_t, N, T...>*>(this_ptr);
            index.destroy();
        }
    };

    template <typename item_t, int N, typename... T> struct CopyAllFunctor<item_t, db0::v_sorted_sequence<item_t, N, T...> > {
        static size_t execute(const void *this_ptr, item_t *where) {
            const db0::v_sorted_sequence<item_t, N, T...> &index =
                    *reinterpret_cast<const db0::v_sorted_sequence<item_t, N, T...>*>(this_ptr);
            auto it = index.begin(), end = index.end();
            while (it!=end) {
                *where = *it;
                ++it;
                ++where;
            }
            return N;
        }
    };

    template<typename DefinitionT, int N, typename... T>
    struct CountUniqueFunctor<DefinitionT, db0::v_sorted_sequence<typename DefinitionT::item_t, N, T...> > {
        static std::size_t execute(
            const void *this_ptr, typename DefinitionT::Containers::IInputRange &input, std::size_t max_count
        )
        {
            using ContainerT = db0::v_sorted_sequence<typename DefinitionT::item_t, N, T...>;
            return countNewGenericImpl<DefinitionT, ContainerT>(this_ptr, input, max_count);
        }
    };

    template <typename item_t, int N, typename... T> struct FindExistingFunctor<item_t, db0::v_sorted_sequence<item_t, N, T...> > {
        static bool execute(const void *this_ptr, const item_t &item) {
            const db0::v_sorted_sequence<item_t, N, T...> &index =
                    *reinterpret_cast<const db0::v_sorted_sequence<item_t, N, T...>*>(this_ptr);
            return index.find(item) != index.end();
        }
    };

    template <typename item_t, int N, typename... T> struct UpdateExistingFunctor<item_t, db0::v_sorted_sequence<item_t, N, T...> >
    {
        static bool execute(void *this_ptr, const item_t &item, item_t *old_item) {
            db0::v_sorted_sequence<item_t, N, T...> &index =
                *reinterpret_cast<db0::v_sorted_sequence<item_t, N, T...>*>(this_ptr);
            return index.updateExisting(item, old_item);
        }
    };

    template <typename item_t, int N, typename... T> struct FindOneFunctor<item_t, db0::v_sorted_sequence<item_t, N, T...> >
    {
        static bool execute(const void *this_ptr, item_t &item) {
            const db0::v_sorted_sequence<item_t, N, T...> &index =
                *reinterpret_cast<const db0::v_sorted_sequence<item_t, N, T...>*>(this_ptr);
            return index.findOne(item);
        }
    };

    template<typename DefinitionT, int N, typename... T>
    struct CountExistingFunctor<DefinitionT, db0::v_sorted_sequence<typename DefinitionT::item_t, N, T...> > {
        static std::size_t execute(
            const void *this_ptr, typename DefinitionT::Containers::IInputRange &input, std::size_t max_count
        )
        {
            using ContainerT = db0::v_sorted_sequence<typename DefinitionT::item_t, N, T...>;
            return countExistingGenericImpl<DefinitionT, ContainerT>(this_ptr, input, max_count);
        }
    };

    template <typename item_t, int N, typename... T> struct CommitFunctor<item_t, db0::v_sorted_sequence<item_t, N, T...> > 
    {
        static void execute(void *this_ptr) {            
            db0::v_sorted_sequence<item_t, N, T...> &index =
                *reinterpret_cast<db0::v_sorted_sequence<item_t, N, T...>*>(this_ptr);            
            index.commit();
        }
    };

    /**
     * v_sorted_vector specialization
     */
    template<typename DefinitionT, typename... T>
    struct BulkInsertUniqueFunctor<DefinitionT, db0::v_sorted_vector<T...>> {
        using CallbackT = std::function<void(typename DefinitionT::item_t)>;
        static std::pair<std::uint32_t, std::uint32_t>
        execute(void *this_ptr, typename DefinitionT::Containers::IInputRange &input, CallbackT *callback_ptr)
        {
            return insertGenericImpl<DefinitionT, db0::v_sorted_vector<T...>>(this_ptr, input, callback_ptr);
        }
    };

    template <typename item_t, typename... T> struct InsertFunctor<item_t, db0::v_sorted_vector<item_t, T...> > {
        static void execute(void *this_ptr, const item_t &item) {
            reinterpret_cast<db0::v_sorted_vector<item_t, T...>*>(this_ptr)->insert(item);
        }
    };

    template<typename DefinitionT, typename... T>
    struct BulkEraseFunctor<DefinitionT, db0::v_sorted_vector<T...> > {
        using CallbackT = std::function<void(typename DefinitionT::item_t)>;
        static std::size_t execute(void *this_ptr, typename DefinitionT::Containers::IInputRange &input, CallbackT *callback_ptr)
        {
            return eraseGenericImpl<DefinitionT, db0::v_sorted_vector<T...>>(this_ptr, input, callback_ptr);
        }
    };

    template <typename item_t, typename... T> struct EraseFunctor<item_t, db0::v_sorted_vector<item_t, T...> > {
        static void execute(void *this_ptr, const item_t &item) {
            auto &index = *reinterpret_cast<db0::v_sorted_vector<item_t, T...>*>(this_ptr);
            auto it = index.find(item);
            // erase item if exists
            if (it) {
                index.eraseItem(it);
            }
        }
    };

    template <typename item_t, typename... T> struct EmptyFunctor<item_t, db0::v_sorted_vector<item_t, T...> > {
        static bool execute(const void *this_ptr) {
            const db0::v_sorted_vector<item_t, T...> &index =
                    *reinterpret_cast<const db0::v_sorted_vector<item_t, T...>*>(this_ptr);
            return index.empty();
        }
    };

    template <typename item_t, typename... T> struct SizeFunctor<item_t, db0::v_sorted_vector<item_t, T...> > {
        static std::uint64_t execute(const void *this_ptr) {
            const db0::v_sorted_vector<item_t, T...> &index =
                    *reinterpret_cast<const db0::v_sorted_vector<item_t, T...>*>(this_ptr);
            return index.size();
        }
    };

    template <typename item_t, typename... T> struct GetAddrFunctor<item_t, db0::v_sorted_vector<item_t, T...> >
    {
        using AddrT = typename db0::v_sorted_vector<item_t, T...>::addr_t;
        static AddrT execute(const void *this_ptr) {
            const db0::v_sorted_vector<item_t, T...> &index =
                    *reinterpret_cast<const db0::v_sorted_vector<item_t, T...>*>(this_ptr);
            return index.getAddress();
        }
    };
    
    template <typename item_t, typename... T> struct BeginFunctor<item_t, db0::v_sorted_vector<item_t, T...> > {
        static std::shared_ptr<void> execute(const void *this_ptr, int direction) 
    {
            const db0::v_sorted_vector<item_t, T...> &index =
                    *reinterpret_cast<const db0::v_sorted_vector<item_t, T...>*>(this_ptr);
            auto it = index.beginJoin(direction);
            return db0::make_shared_void<decltype(it)>(it);
        }
    };

    template <typename item_t, typename... T> struct DestroyFunctor<item_t, db0::v_sorted_vector<item_t, T...> > {
        static void execute(void *this_ptr, Memspace &) {
            db0::v_sorted_vector<item_t, T...> &index = *reinterpret_cast<db0::v_sorted_vector<item_t, T...>*>(this_ptr);
            index.destroy();
        }
    };

    template <typename item_t, typename... T> struct SizeOfFunctor<item_t, db0::v_sorted_vector<item_t, T...> > {
        static std::uint64_t execute(const void *this_ptr) {
            const db0::v_sorted_vector<item_t, T...> &index =
                    *reinterpret_cast<const db0::v_sorted_vector<item_t, T...>*>(this_ptr);
            return index.sizeOf();
        }
    };

    template <typename item_t, typename... T> struct CopyAllFunctor<item_t, db0::v_sorted_vector<item_t, T...> > {
        static size_t execute(const void *this_ptr, item_t *where) {
            const db0::v_sorted_vector<item_t, T...> &index =
                    *reinterpret_cast<const db0::v_sorted_vector<item_t, T...>*>(this_ptr);
            auto it = index.begin(), end = index.end();
            item_t *out = where;
            while (it!=end) {
                *out = *it;
                ++it;
                ++out;
            }
            return (out - where);
        }
    };

    template<typename DefinitionT, typename... T>
    struct CountUniqueFunctor<DefinitionT, db0::v_sorted_vector<T...>> {
        static std::size_t execute(
            const void *this_ptr, typename DefinitionT::Containers::IInputRange &input, std::size_t max_count
        )
        {
            return countNewGenericImpl<DefinitionT, db0::v_sorted_vector<T...>>(this_ptr, input, max_count);
        }
    };

    template <typename item_t, typename... T> struct FindExistingFunctor<item_t, db0::v_sorted_vector<item_t, T...> > {
        static bool execute(const void *this_ptr, const item_t &item) {
            const db0::v_sorted_vector<item_t, T...> &index =
                    *reinterpret_cast<const db0::v_sorted_vector<item_t, T...>*>(this_ptr);
            return index.find(item) != index.end();
        }
    };

    template <typename item_t, typename... T> struct UpdateExistingFunctor<item_t, db0::v_sorted_vector<item_t, T...> >
    {
        static bool execute(void *this_ptr, const item_t &item, item_t *old_item) {
            db0::v_sorted_vector<item_t, T...> &index =
                *reinterpret_cast<db0::v_sorted_vector<item_t, T...>*>(this_ptr);
            return index.updateExisting(item, old_item);
        }
    };

    template <typename item_t, typename... T> struct FindOneFunctor<item_t, db0::v_sorted_vector<item_t, T...> >
    {
        static bool execute(const void *this_ptr, item_t &item) {
            const db0::v_sorted_vector<item_t, T...> &index =
                *reinterpret_cast<const db0::v_sorted_vector<item_t, T...>*>(this_ptr);
            return index.findOne(item);
        }
    };

    template<typename DefinitionT, typename... T>
    struct CountExistingFunctor<DefinitionT, db0::v_sorted_vector<T...>> {
        static std::size_t execute(
            const void *this_ptr, typename DefinitionT::Containers::IInputRange &input, std::size_t max_count
        )
        {
            return countExistingGenericImpl<DefinitionT, db0::v_sorted_vector<T...>>(this_ptr, input, max_count);
        }
    };

    template <typename item_t, typename... T> struct CommitFunctor<item_t, db0::v_sorted_vector<item_t, T...> > 
    {
        static void execute(void *this_ptr) {
            db0::v_sorted_vector<item_t, T...> &index = *reinterpret_cast<db0::v_sorted_vector<item_t, T...>*>(this_ptr);            
            index.commit();
        }
    };

    /**
     * itty_index specialization
     */
    template<typename DefinitionT, typename... T>
    struct BulkInsertUniqueFunctor<DefinitionT, db0::IttyIndex<T...> > {
        using CallbackT = std::function<void(typename DefinitionT::item_t)>;
        static std::pair<std::uint32_t, std::uint32_t>
        execute(void *this_ptr, typename DefinitionT::Containers::IInputRange &input, CallbackT *callback_ptr)
        {
            return insertGenericImpl<DefinitionT, db0::IttyIndex<T...>>(this_ptr, input, callback_ptr);
        }
    };
    
    template <typename item_t, typename... T> struct InsertFunctor<item_t, db0::IttyIndex<item_t, T...> >
    {
        static void execute(void *, const item_t &) {
            THROWF(db0::InternalException) << "Cannot insert to immutable collection";
        }
    };
    
    template<typename DefinitionT, typename... T>
    struct BulkEraseFunctor<DefinitionT, db0::IttyIndex<T...> > {
        using CallbackT = std::function<void(typename DefinitionT::item_t)>;
        static std::size_t execute(void *this_ptr, typename DefinitionT::Containers::IInputRange &input, CallbackT *callback_ptr)
        {
            return eraseGenericImpl<DefinitionT, db0::IttyIndex<T...>>(this_ptr, input, callback_ptr);
        }
    };

    template <typename item_t, typename... T> struct EraseFunctor<item_t, db0::IttyIndex<item_t, T...> >
    {
        static void execute(void *, const item_t &) {
            THROWF(db0::InternalException) << "Erase operation not permitted on itty_index" << THROWF_END;
        }
    };

    template <typename item_t, typename... T> struct EmptyFunctor<item_t, db0::IttyIndex<item_t, T...> >
    {
        static bool execute(const void *) {
            return false;
        }
    };

    template <typename item_t, typename... T> struct SizeFunctor<item_t, db0::IttyIndex<item_t, T...> > 
    {
        static std::uint64_t execute(const void *) {
            return 1;
        }
    };

    template <typename item_t, typename... T> struct GetAddrFunctor<item_t, db0::IttyIndex<item_t, T...> >
    {
        using AddrT = typename db0::IttyIndex<item_t, T...>::addr_t;
        static AddrT execute(const void *this_ptr) {
            const db0::IttyIndex<item_t, T...> &index = *reinterpret_cast<const db0::IttyIndex<item_t, T...>*>(this_ptr);
            return index.getAddress();
        }
    };

    template <typename item_t, typename... T> struct BeginFunctor<item_t, db0::IttyIndex<item_t, T...> > 
    {
        using index_type = db0::IttyIndex<item_t, T...>;
        using iterator_type = typename index_type::joinable_const_iterator;

        static std::shared_ptr<void> execute(const void *this_ptr, int direction) {
            const auto &index = *reinterpret_cast<const index_type*>(this_ptr);
            return db0::make_shared_void<iterator_type>(index.beginJoin(direction));
        }
    };

    template <typename item_t, typename... T> struct DestroyFunctor<item_t, db0::IttyIndex<item_t, T...> > 
    {
        static void execute(void *, Memspace &) {
        }
    };

    template <typename item_t, typename... T> struct SizeOfFunctor<item_t, db0::IttyIndex<item_t, T...> > 
    {
        static std::uint64_t execute(const void *) {
            return db0::IttyIndex<item_t, T...>::getStorageSize();
        }
    };

    template <typename item_t, typename... T> struct CopyAllFunctor<item_t, db0::IttyIndex<item_t, T...> > 
    {
        static std::size_t execute(const void *this_ptr, item_t *where) {
            const db0::IttyIndex<item_t, T...> &index =
                *reinterpret_cast<const db0::IttyIndex<item_t, T...>*>(this_ptr);
            *where = index.getValue();
            return 1;
        }
    };

    template<typename DefinitionT, typename... T>
    struct CountUniqueFunctor<DefinitionT, db0::IttyIndex<T...> > {
        static size_t execute(
            const void *this_ptr, typename DefinitionT::Containers::IInputRange &input, std::size_t max_count
        )
        {
            return countNewGenericImpl<DefinitionT, db0::IttyIndex<T...>>(this_ptr, input, max_count);
        }
    };

    template <typename item_t, typename... T> struct FindExistingFunctor<item_t, db0::IttyIndex<item_t, T...> > 
    {
        using index_type = db0::IttyIndex<item_t, T...>;
        using CompT = typename index_type::CompT;
        static bool execute(const void *this_ptr, const item_t &item) {
            const db0::IttyIndex<item_t, T...> &index =
                *reinterpret_cast<const db0::IttyIndex<item_t, T...>*>(this_ptr);
            return !(CompT()(index.getValue(), item) || CompT()(item, index.getValue()));
        }
    };
    
    template <typename item_t, typename... T> struct UpdateExistingFunctor<item_t, db0::IttyIndex<item_t, T...> >
    {
        static bool execute(void *this_ptr, const item_t &item, item_t *old_item) {
            db0::IttyIndex<item_t, T...> &index =
                *reinterpret_cast<db0::IttyIndex<item_t, T...>*>(this_ptr);
            return index.updateExisting(item, old_item);
        }
    };

    template <typename item_t, typename... T> struct FindOneFunctor<item_t, db0::IttyIndex<item_t, T...> >
    {
        static bool execute(const void *this_ptr, item_t &item) {
            const db0::IttyIndex<item_t, T...> &index =
                *reinterpret_cast<const db0::IttyIndex<item_t, T...>*>(this_ptr);
            return index.findOne(item);
        }
    };

    template<typename DefinitionT, typename... T>
    struct CountExistingFunctor<DefinitionT, db0::IttyIndex<T...>> {
        static std::size_t execute(
            const void *this_ptr, typename DefinitionT::Containers::IInputRange &input, size_t max_count
        )
        {
            return countExistingGenericImpl<DefinitionT, db0::IttyIndex<T...>>(this_ptr, input, max_count);
        }
    };

    template <typename item_t, typename... T> struct CommitFunctor<item_t, db0::IttyIndex<item_t, T...> > 
    {
        static void execute(void *) {
        }
    };

    /**
     * empty_index specializations
     */
    template<typename DefinitionT, typename... T>
    struct BulkInsertUniqueFunctor<DefinitionT, db0::empty_index<T...> > 
    {
        using CallbackT = std::function<void(typename DefinitionT::item_t)>;
        static std::pair<std::uint32_t, std::uint32_t>
        execute(void *this_ptr, typename DefinitionT::Containers::IInputRange &input, CallbackT *callback_ptr)
        {
            return insertGenericImpl<DefinitionT, db0::empty_index<T...> >(this_ptr, input, callback_ptr);
        }
    };
    
    template <typename item_t, typename... T>
    struct InsertFunctor<item_t, db0::empty_index<T...> >
    {
        static void execute(void *, const item_t &) {
            THROWF(db0::InternalException) << "Cannot insert to immutable collection";
        }
    };

    template<typename DefinitionT, typename... T>
    struct BulkEraseFunctor<DefinitionT, db0::empty_index<T...> > 
    {
        using CallbackT = std::function<void(typename DefinitionT::item_t)>;
        static std::size_t execute(void *this_ptr, typename DefinitionT::Containers::IInputRange &input, CallbackT *callback_ptr)
        {
            return eraseGenericImpl<DefinitionT, db0::empty_index<T...> >(this_ptr, input, callback_ptr);
        }
    };

    template <typename item_t, typename... T> struct EraseFunctor<item_t, db0::empty_index<T...> > {
        static void execute(void *, const item_t &) {
            THROWF(db0::InternalException) << "Erase operation not permitted on empty_index" << THROWF_END;
        }
    };

    template <typename item_t, typename... T> struct EmptyFunctor<item_t, db0::empty_index<T...> > {
        static bool execute(const void *) {
            return true;
        }
    };

    template <typename item_t, typename... T> struct CopyAllFunctor<item_t, db0::empty_index<T...> > {
        static std::size_t execute(const void *, item_t *) {
            return 0;
        }
    };

    template <typename item_t, typename... T> struct SizeFunctor<item_t, db0::empty_index<T...> > {
        static std::uint64_t execute(const void *) {
            return 0;
        }
    };

    template <typename item_t, typename... T> struct GetAddrFunctor<item_t, db0::empty_index<T...> >
    {
        using AddrT = typename db0::empty_index<T...>::addr_t;
        static AddrT execute(const void *) {
            return {};
        }
    };

    template <typename item_t, typename... T> struct BeginFunctor<item_t, db0::empty_index<T...> > {
        static std::shared_ptr<void> execute(const void *, int) {
            return db0::make_shared_void<typename empty_index<T...>::const_iterator>(empty_index<T...>::begin());
        }
    };

    template <typename item_t, typename... T> struct DestroyFunctor<item_t, db0::empty_index<T...> > {
        static void execute(void *, Memspace &) {
        }
    };

    template <typename item_t, typename... T> struct SizeOfFunctor<item_t, db0::empty_index<T...> > {
        static std::uint64_t execute(const void *) {
            return 0;
        }
    };

    template<typename DefinitionT, typename... T>
    struct CountUniqueFunctor<DefinitionT, db0::empty_index<T...> > 
    {
        static std::size_t execute(
            const void *this_ptr, typename DefinitionT::Containers::IInputRange &input, std::size_t max_count
        )
        {
            return countNewGenericImpl<DefinitionT, db0::empty_index<T...> >(this_ptr, input, max_count);
        }
    };

    template <typename item_t, typename... T> struct FindExistingFunctor<item_t, db0::empty_index<T...> > {
        static bool execute(const void *, const item_t &) {
            return false;
        }
    };

    template <typename item_t, typename... T> struct UpdateExistingFunctor<item_t, db0::empty_index<T...> > {
        static bool execute(void *, const item_t &, item_t *) {
            return false;
        }
    };

    template <typename item_t, typename... T> struct FindOneFunctor<item_t, db0::empty_index<T...> > {
        static bool execute(const void *, item_t &) {
            return false;
        }
    };

    template<typename DefinitionT, typename... T> struct CountExistingFunctor<DefinitionT, db0::empty_index<T...> > {
        static std::size_t execute(
            const void *this_ptr, typename DefinitionT::Containers::IInputRange &input, std::size_t max_count
        )
        {
            return countExistingGenericImpl<DefinitionT, db0::empty_index<T...> >(this_ptr, input, max_count);
        }
    };

    template <typename item_t, typename... T> struct CommitFunctor<item_t, db0::empty_index<T...> > {
        static void execute(void *) {
        }
    };

    /**
     * Implementation of common v_bindex interface
     */
    template <typename DefinitionT> class Impl
    {
    public :
        using item_t = typename DefinitionT::item_t;
        using AddrT = typename DefinitionT::addr_t;

        Impl() = default;

        /**
         * @param ref passed to hold persistency
         */
        template <typename T> Impl(T*, std::shared_ptr<void> ref)
            : m_ref(ref)
            , m_ptr(m_ref.get())
            , m_bulk_insert_unique_ptr(BulkInsertUniqueFunctor<DefinitionT, T>::execute)
            , m_insert_ptr(InsertFunctor<item_t, T>::execute)
            , m_bulk_erase_ptr(BulkEraseFunctor<DefinitionT, T>::execute)
            , m_empty_ptr(EmptyFunctor<item_t, T>::execute)
            , m_get_addr_ptr(GetAddrFunctor<item_t, T>::execute)
            , m_count_unique_ptr(CountUniqueFunctor<DefinitionT, T>::execute)
            , m_find_existing_ptr(FindExistingFunctor<item_t, T>::execute)
            , m_update_existing_ptr(UpdateExistingFunctor<item_t, T>::execute)
            , m_find_one_ptr(FindOneFunctor<item_t, T>::execute)
            , m_count_existing_ptr(CountExistingFunctor<DefinitionT, T>::execute)
            , m_size_ptr(SizeFunctor<item_t, T>::execute)
            , m_copy_all_ptr(CopyAllFunctor<item_t, T>::execute)
            , m_size_of_ptr(SizeOfFunctor<item_t, T>::execute)
            , m_destroy_ptr(DestroyFunctor<item_t, T>::execute)
            , m_begin_ptr(BeginFunctor<item_t, T>::execute)            
            , m_erase_ptr(EraseFunctor<item_t, T>::execute)            
            , m_commit_ptr(CommitFunctor<item_t, T>::execute)
        {}
        
        /**
         * @return items requested / items actually inserted
         */
        template <typename ItemT, typename InputIterator>
        std::pair<std::uint32_t, std::uint32_t> bulkInsertUnique(InputIterator begin, InputIterator end,
            std::function<void(ItemT)> *callback_ptr)
        {
            db0::bindex::GenericInputRange<InputIterator, DefinitionT> input(begin, end);
            return m_bulk_insert_unique_ptr(m_ptr, input, callback_ptr);
        }
        
        /**
         * Insert single item
         */
        void insert(const item_t &item) {
            return m_insert_ptr(m_ptr, item);
        }

        template <typename ItemT, typename InputIterator>
        std::size_t bulkErase(InputIterator begin, InputIterator end, std::function<void(ItemT)> *callback_ptr)
        {
            db0::bindex::GenericInputRange<InputIterator, DefinitionT> input(begin, end);
            return m_bulk_erase_ptr(m_ptr, input, callback_ptr);
        }

        void erase(const item_t &item) {
            return m_erase_ptr(m_ptr, item);
        }

        /**
         * @param max_count bound to not exceed
         * (since collection may be large it makes no sense to iterate past this mark)
         * @return number of unique (not present in current collection) elements \
         * passed in the input collection specified by begin / end iterators
         */
        template<typename InputIterator>
        std::size_t countUnique(InputIterator begin, InputIterator end, int max_count) const
        {
            db0::bindex::GenericInputRange<InputIterator, DefinitionT> input(begin, end);
            return m_count_unique_ptr(m_ptr, input, max_count);
        }

        /**
         * Check if specific element exists in collection
         * @return true if found
         */
        bool findExisting(const item_t &item) const {
            return m_find_existing_ptr(m_ptr, item);
        }

        /**
         * Update existing element in collection
         * @return true if found and updated
        */
        bool updateExisting(const item_t &item, item_t *old_item = nullptr) {
            return m_update_existing_ptr(m_ptr, item, old_item);
        }
        
        bool findOne(item_t &item) const {
            return m_find_one_ptr(m_ptr, item);
        }

        template<typename InputIterator>
        std::size_t countExisting(InputIterator begin, InputIterator end, int max_count) const
        {
            db0::bindex::GenericInputRange<InputIterator, DefinitionT> input(begin, end);
            return m_count_existing_ptr(m_ptr, input, max_count);
        }

        /**
         * @return precise collection size (slow for v_bindex)
         */
        std::size_t size() const {
            return m_size_ptr(m_ptr);
        }

        /**
         * Read all contents from data collection and store under "where"
         * @return number of elements copied
         */
        std::size_t copyAll(item_t *where) const {
            return m_copy_all_ptr(m_ptr, where);
        }

        /**
         * @return true if collection is empty
         */
        bool empty() const {
            return m_empty_ptr(m_ptr);
        }

        /**
         * @return size of the entire data structure in bytes
         */
        std::uint64_t sizeOf() const {
            return m_size_of_ptr(m_ptr);
        }

        /**
         * Destroys corresponding v-space instance
         */
        void destroy(Memspace &memspace) const {
            m_destroy_ptr(m_ptr, memspace);
        }

        std::shared_ptr<void> beginJoin(int direction) const {
            return m_begin_ptr(m_ptr, direction);
        }

        AddrT getAddress() const {
            return m_get_addr_ptr(m_ptr);
        }
        
        void commit() const {
            m_commit_ptr(m_ptr);
        }

    private:
        std::shared_ptr<void> m_ref;
        // pointer to actual data collection (persisted in m_ref)
        void *m_ptr = nullptr;
        bulkInsertUniquePtr<DefinitionT> m_bulk_insert_unique_ptr = nullptr;
        insertPtr<item_t> m_insert_ptr = nullptr;
        bulkErasePtr<DefinitionT> m_bulk_erase_ptr = nullptr;
        emptyPtr<item_t> m_empty_ptr = nullptr;
        getAddrPtr<item_t, AddrT> m_get_addr_ptr = nullptr;
        countUniquePtr<DefinitionT> m_count_unique_ptr = nullptr;
        findExistingPtr<item_t> m_find_existing_ptr = nullptr;
        updateExistingPtr<item_t> m_update_existing_ptr = nullptr;
        findOnePtr<item_t> m_find_one_ptr = nullptr;
        countExistingPtr<DefinitionT> m_count_existing_ptr = nullptr;
        sizePtr<item_t> m_size_ptr = nullptr;
        copyAllPtr<item_t> m_copy_all_ptr = nullptr;
        sizeOfPtr<item_t> m_size_of_ptr = nullptr;
        destroyPtr<item_t> m_destroy_ptr = nullptr;
        beginPtr<item_t> m_begin_ptr = nullptr;        
        erasePtr<item_t> m_erase_ptr = nullptr;        
        commitPtr<item_t> m_commit_ptr = nullptr;
    };

} 
