// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "BlockIOStream.hpp"
#include <dbzero/core/serialization/Base.hpp>
#include <dbzero/core/collections/rle/RLE_Sequence.hpp>
#include <dbzero/core/compiler_attributes.hpp>
#include <dbzero/core/serialization/Ext.hpp>
#include <dbzero/core/serialization/Types.hpp>

namespace db0

{
    
    class ChangeLogData
    {
    public:
        RLE_SequenceBuilder<std::uint64_t> m_rle_builder;
        std::vector<std::uint64_t> m_change_log;
        
        ChangeLogData() = default;
        
        /**
         * @param change_log the list of modified addresses
         * @param rle_compress flag indicating if RLE encoding/compression should be applied
         * @param add_duplicates flag indicating if duplicates should be added to the change log
         * @param is_sorted flag indicating if the change log is already sorted         
        */
        ChangeLogData(const std::vector<std::uint64_t> &change_log, bool rle_compress, bool add_duplicates, bool is_sorted);
        ChangeLogData(std::vector<std::uint64_t> &&change_log, bool rle_compress, bool add_duplicates, bool is_sorted);

    private:
        void initRLECompress(bool is_sorted, bool add_duplicates);
    };
    
    // @tparam BaseT base class type (for storing common header info if needed)
DB0_PACKED_BEGIN
    template <typename BaseT = db0::o_fixed_null>
    struct DB0_PACKED_ATTR o_change_log: public o_ext<o_change_log<BaseT>, BaseT, 0, false>
    {
    protected:
        using super_t = o_ext<o_change_log<BaseT>, BaseT, 0, false>;
        friend super_t;
        
        template <typename... Args>
        o_change_log(const ChangeLogData &, Args&&... args);

        // change log as RLE sequence type
        const o_rle_sequence<std::uint64_t> &rle_sequence() const
        {
            return reinterpret_cast<const o_rle_sequence<std::uint64_t> &>(
                this->getDynAfter(rleCompressed(), o_rle_sequence<std::uint64_t>::type())
            );
        }
        
        // uncompressed change log
        const o_list<o_simple<std::uint64_t> > &changle_log() const {
            return this->getDynAfter(rleCompressed(), o_list<o_simple<std::uint64_t> >::type());
        }

    public:
        template <typename... Args>
        static std::size_t measure(const ChangeLogData &, Args... args);

        const o_simple<bool> &rleCompressed() const {
            return this->getDynFirst(o_simple<bool>::type());
        }

        class ConstIterator
        {
        public:
            ConstIterator(o_list<o_simple<std::uint64_t> >::const_iterator it);
            ConstIterator(o_rle_sequence<std::uint64_t>::ConstIterator it);

            ConstIterator &operator++();
            std::uint64_t operator*() const;
            bool operator!=(const ConstIterator &other) const;

        private:
            bool m_rle;
            o_list<o_simple<std::uint64_t> >::const_iterator m_list_it;
            o_rle_sequence<std::uint64_t>::ConstIterator m_rle_it;
        };
        
        bool isRLECompressed() const;
        
        ConstIterator begin() const;
        ConstIterator end() const;
        
        template <typename T> static std::size_t safeSizeOf(T buf)
        {
            auto _buf = buf;
            buf += super_t::safeBaseSize(buf);
            auto is_rle_compressed = o_simple<bool>::__const_ref(buf);
            buf += is_rle_compressed.sizeOf();
            if (is_rle_compressed.value()) {
                buf += o_rle_sequence<std::uint64_t>::safeSizeOf(buf);
            } else {
                buf += o_list<o_simple<std::uint64_t> >::safeSizeOf(buf);            
            }
            return buf - _buf;
        }
    };
DB0_PACKED_END
    
    template <typename BaseT>
    template <typename... Args>
    o_change_log<BaseT>::o_change_log(const ChangeLogData &data, Args&&... args)
        : super_t(std::forward<Args>(args)...)
    {
        bool rle_compressed = !data.m_rle_builder.empty();
        if (rle_compressed) {
            this->arrangeMembers()
                (o_simple<bool>::type(), true)
                (o_rle_sequence<std::uint64_t>::type(), data.m_rle_builder.getData());
        } else {
            this->arrangeMembers()
                (o_simple<bool>::type(), false)
                (o_list<o_simple<std::uint64_t> >::type(), data.m_change_log);
        }
    }
    
    template <typename BaseT>
    template <typename... Args>
    std::size_t o_change_log<BaseT>::measure(const ChangeLogData &data, Args... args)
    {
        bool rle_compressed = !data.m_rle_builder.empty();
        if (rle_compressed) {
            return super_t::measureMembersFromBase(std::forward<Args>(args)...)
                (o_simple<bool>::type())
                (o_rle_sequence<std::uint64_t>::type(), data.m_rle_builder.getData());
        } else {
            return super_t::measureMembersFromBase(std::forward<Args>(args)...)
                (o_simple<bool>::type())
                (o_list<o_simple<std::uint64_t> >::type(), data.m_change_log);
        }
    }
    
    extern template class o_change_log<>;

}