#pragma once

#include "BlockIOStream.hpp"
#include <dbzero/core/serialization/Base.hpp>
#include <dbzero/core/collections/rle/RLE_Sequence.hpp>

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
    
    class [[gnu::packed]] o_change_log: public o_base<o_change_log, 0, false>
    {
    protected:
        friend struct o_base<o_change_log, 0, false>;
        bool m_rle_compressed;
        
        o_change_log(const ChangeLogData &);

        // change log as RLE sequence type
        const o_rle_sequence<std::uint64_t> &rle_sequence() const
        {
            return reinterpret_cast<const o_rle_sequence<std::uint64_t> &>(this->getDynFirst(
                o_rle_sequence<std::uint64_t>::type()));
        }
        
        // uncompressed change log
        const o_list<o_simple<std::uint64_t> > &changle_log() const {
            return this->getDynFirst(o_list<o_simple<std::uint64_t> >::type());
        }

    public:
        static std::size_t measure(const ChangeLogData &);

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
        
        ConstIterator begin() const;
        ConstIterator end() const;

        // Decode the last element from the log (possibly a sentinel)
        std::uint64_t last() const;

        bool isRLECompressed() const;

        template <typename T> static std::size_t safeSizeOf(T buf)
        {
            auto _buf = buf;
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
        
}