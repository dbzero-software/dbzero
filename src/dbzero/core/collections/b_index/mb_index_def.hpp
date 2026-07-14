// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "empty_index.hpp"
#include "IttyIndex.hpp"
#include "v_bindex.hpp"
#include <dbzero/core/metaprog/Multimorph.hpp>
#include <dbzero/core/collections/vector/v_sorted_vector.hpp>
#include <dbzero/core/collections/vector/v_sorted_sequence.hpp>

namespace db0::bindex

{

    template <typename ItemT, typename AddrT, typename Compare>
    struct MorphingBIndexDefinition
    {
        using item_t = ItemT;
        using addr_t = AddrT;
        using item_comp_t = Compare;

        using empty_t = empty_index<AddrT>;
        using itty_index_t = IttyIndex<item_t, AddrT>;
        // array consisting of 2 elements
        using array2_t = v_sorted_sequence<item_t, 2, AddrT, item_comp_t>;
        // array consisting of 3 elements
        using array3_t = v_sorted_sequence<item_t, 3, AddrT, item_comp_t>;
        // array consisting of 4 elements
        using array4_t = v_sorted_sequence<item_t, 4, AddrT, item_comp_t>;
        using vector_t = v_sorted_vector<item_t, AddrT, item_comp_t>;
        using bindex_t = v_bindex<item_t, AddrT, item_comp_t>;
        using CallbackT = std::function<void(item_t)>;

        template<typename ContainerType>
        class IContainerInputRange 
        {
        public:
            virtual ~IContainerInputRange() = default;

            virtual std::pair<std::uint32_t, std::uint32_t> insert(ContainerType&, CallbackT *callback_ptr) = 0;
            virtual std::size_t erase(ContainerType&, CallbackT *callback_ptr) = 0;
            virtual std::size_t countNew(const ContainerType&, std::size_t max_count) = 0;
            virtual std::size_t countExisting(const ContainerType&, std::size_t max_count) = 0;
        };

        template<typename... Ts> struct ContainersDefinition
        {
            template<typename Interface>
            using Multimorph = db0::Multimorph<Interface, Ts...>;

            class IInputRange : public IContainerInputRange<Ts>... {
            public:
                virtual ~IInputRange() = default;
            };
        };
        
        using Containers = ContainersDefinition<
            empty_t, itty_index_t, array2_t, array3_t, array4_t, vector_t, bindex_t
        >;
    
    };
    
}
