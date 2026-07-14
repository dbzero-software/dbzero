// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 DBZero Software sp. z o.o.

#pragma once

#include "FT_Iterator.hpp"
#include <dbzero/core/collections/b_index/mb_index.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/serialization/Serializable.hpp>
#include <ostream>
#include <vector>

namespace db0

{

    template <typename key_t = UniqueAddress, typename IndexKeyT = std::uint64_t>
    class FT_MissingIndexIterator: public FT_Iterator<key_t>
    {
    public:
        using self_t = FT_MissingIndexIterator<key_t, IndexKeyT>;
        using super_t = FT_Iterator<key_t>;

        FT_MissingIndexIterator(std::uint64_t fixture_uuid, int direction,
            std::vector<IndexKeyT> &&index_key_sequence)
            : m_fixture_uuid(fixture_uuid)
            , m_direction(direction)
            , m_index_key_sequence(std::move(index_key_sequence))
        {
        }

        key_t getKey() const override
        {
            THROWF(db0::InputException) << "Missing index iterator has no key" << THROWF_END;
        }

        bool isEnd() const override
        {
            return true;
        }

        const std::type_info &typeId() const override
        {
            return typeid(self_t);
        }

        void operator++() override
        {
        }

        void operator--() override
        {
        }

        void next(void * = nullptr) override
        {
        }

        bool join(key_t, int = -1) override
        {
            return false;
        }

        void joinBound(key_t) override
        {
        }

        std::pair<key_t, bool> peek(key_t) const override
        {
            return { key_t{}, false };
        }

        bool isNextKeyDuplicated() const override
        {
            return false;
        }

        std::unique_ptr<FT_Iterator<key_t> > beginTyped(int direction = -1) const override
        {
            return std::make_unique<self_t>(
                m_fixture_uuid,
                direction,
                std::vector<IndexKeyT>(m_index_key_sequence)
            );
        }

        bool limitBy(key_t) override
        {
            return false;
        }

        std::ostream &dump(std::ostream &os) const override
        {
            return os << "FTMissingIndex@" << this;
        }

        void stop() override
        {
        }

        FTIteratorType getSerialTypeId() const override
        {
            return FTIteratorType::Index;
        }

        void getSignature(std::vector<std::byte> &v) const override
        {
            db0::serial::getSignature(*this, v);
        }

    protected:
        void serializeFTIterator(std::vector<std::byte> &v) const override
        {
            using TypeIdType = decltype(db0::serial::typeId<void>());

            if (m_index_key_sequence.empty()) {
                THROWF(db0::InternalException) << "Missing index iterator is missing index keys" << THROWF_END;
            }

            db0::serial::write<TypeIdType>(v, db0::MorphingBIndex<key_t>::getSerialTypeId());
            db0::serial::write<TypeIdType>(v, db0::serial::typeId<key_t>());
            db0::serial::write<TypeIdType>(v, db0::serial::typeId<IndexKeyT>());
            db0::serial::write(v, m_fixture_uuid);
            db0::serial::write<std::int8_t>(v, m_direction);
            db0::serial::write<std::uint32_t>(v, m_index_key_sequence.size());
            for (auto const &index_key: m_index_key_sequence) {
                db0::serial::write(v, index_key);
            }
        }

        double compareToImpl(const FT_IteratorBase &it) const override
        {
            return this->typeId() == it.typeId() ? 0.0 : 1.0;
        }

    private:
        const std::uint64_t m_fixture_uuid;
        const int m_direction;
        const std::vector<IndexKeyT> m_index_key_sequence;
    };

}
