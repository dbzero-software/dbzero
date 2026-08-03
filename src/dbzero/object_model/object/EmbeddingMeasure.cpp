// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#include "EmbeddingMeasure.hpp"

#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/object_model/dict/o_dict.hpp>
#include <dbzero/object_model/dict/o_py_dict.hpp>
#include <dbzero/object_model/item/Item.hpp>
#include <dbzero/object_model/list/List.hpp>
#include <dbzero/object_model/object/ObjectAnyBase.hpp>
#include <dbzero/object_model/object/ObjectImmutableImpl.hpp>
#include <dbzero/object_model/object/o_embedded_object.hpp>
#include <dbzero/object_model/set/o_py_set.hpp>
#include <dbzero/object_model/tuple/Tuple.hpp>
#include <dbzero/object_model/tuple/o_py_tuple.hpp>
#include <dbzero/object_model/tuple/o_tuple.hpp>
#include <dbzero/core/collections/vector/v_bdata_block.hpp>
#include <dbzero/core/memory/SlabAllocatorConfig.hpp>

#include <algorithm>
#include <limits>

namespace db0::object_model
{
    namespace
    {
        using TypeId = db0::bindings::TypeId;
        using LangToolkit = LangConfig::LangToolkit;
        using ObjectPtr = LangConfig::ObjectPtr;
        using MemoImmutableObject = typename LangToolkit::TypeManager::MemoImmutableObject;

        constexpr std::size_t ALLOCATION_COST = 64;
        constexpr std::size_t PAGE_FETCH_COST = SlabAllocatorConfig::DEFAULT_PAGE_SIZE / 2;

        EmbeddingMeasure makeMeasure(
            StorageClass storageClass, std::size_t embeddedBytes, std::size_t separateStorageBytes = 0,
            std::uint32_t allocationsAvoided = 0, bool requiresObjectView = false, bool requiresCollectionView = false
        )
        {
            return {
                storageClass,
                embeddedBytes,
                separateStorageBytes,
                allocationsAvoided,
                requiresObjectView,
                requiresCollectionView
            };
        }

        o_tuple_item::Element stringElement(ObjectPtr value)
        {
            auto &typeManager = LangToolkit::getTypeManager();
            return o_tuple_item::Element::string(typeManager.extractString(value));
        }

        o_tuple_item::Element bytesElement(ObjectPtr value)
        {
            auto &typeManager = LangToolkit::getTypeManager();
            auto bytes = typeManager.extractBytes(value);
            return o_tuple_item::Element::bytes(bytes.m_data, bytes.m_size);
        }

        std::uint32_t saturatedAdd(std::uint32_t left, std::uint32_t right)
        {
            if (right > std::numeric_limits<std::uint32_t>::max() - left) {
                return std::numeric_limits<std::uint32_t>::max();
            }
            return left + right;
        }

        std::uint32_t allocationsForBytes(std::size_t bytes)
        {
            return static_cast<std::uint32_t>(std::max<std::size_t>(
                1, (bytes + SlabAllocatorConfig::DEFAULT_PAGE_SIZE - 1) / SlabAllocatorConfig::DEFAULT_PAGE_SIZE
            ));
        }

        std::size_t extraPagesFetched(std::size_t embeddedBytes)
        {
            return embeddedBytes / SlabAllocatorConfig::DEFAULT_PAGE_SIZE;
        }

        std::uint32_t listRootAllocationsAvoided(std::size_t itemCount)
        {
            if (itemCount == 0) {
                return 1;
            }

            auto dataBlockCapacity = std::size_t{1}
                << db0::o_block_data<o_typed_item, 0>::shift(SlabAllocatorConfig::DEFAULT_PAGE_SIZE);
            return static_cast<std::uint32_t>(
                1 + (itemCount - 1) / dataBlockCapacity
            );
        }

        std::uint32_t tupleRootAllocationsAvoided(std::size_t itemCount)
        {
            return allocationsForBytes(o_db0_tuple::measure(itemCount));
        }

        std::uint32_t nestedAllocationsAvoided(TypeId typeId, ObjectPtr value);

        std::uint32_t iterableAllocationsAvoided(ObjectPtr value)
        {
            std::uint32_t result = 0;
            auto &typeManager = LangToolkit::getTypeManager();
            auto iterator = LangToolkit::getIterator(value);
            auto item = LangToolkit::next(iterator.get());
            while (!!item) {
                result = saturatedAdd(result, nestedAllocationsAvoided(typeManager.getTypeId(item.get()), item.get()));
                item = LangToolkit::next(iterator.get());
            }
            return result;
        }

        std::uint32_t dictAllocationsAvoided(ObjectPtr value)
        {
            std::uint32_t result = 0;
            auto &typeManager = LangToolkit::getTypeManager();
            auto iterator = LangToolkit::getIterator(value);
            auto key = LangToolkit::next(iterator.get());
            while (!!key) {
                auto dictValue = LangToolkit::getMappingItem(value, key.get());
                result = saturatedAdd(result, nestedAllocationsAvoided(typeManager.getTypeId(key.get()), key.get()));
                result = saturatedAdd(
                    result, nestedAllocationsAvoided(typeManager.getTypeId(dictValue.get()), dictValue.get())
                );
                key = LangToolkit::next(iterator.get());
            }
            return result;
        }

        std::uint32_t immutableMemoAllocationsAvoided(ObjectPtr value);

        std::uint32_t nestedAllocationsAvoided(TypeId typeId, ObjectPtr value)
        {
            switch (typeId) {
                case TypeId::STRING:
                case TypeId::BYTES:
                case TypeId::BYTES_ARRAY:
                    return 1;
                case TypeId::LIST:
                    return saturatedAdd(listRootAllocationsAvoided(LangToolkit::length(value)), iterableAllocationsAvoided(value));
                case TypeId::TUPLE:
                    return saturatedAdd(tupleRootAllocationsAvoided(LangToolkit::length(value)), iterableAllocationsAvoided(value));
                case TypeId::SET:
                    return saturatedAdd(1, iterableAllocationsAvoided(value));
                case TypeId::DICT:
                    return saturatedAdd(1, dictAllocationsAvoided(value));
                case TypeId::MEMO_IMMUTABLE_OBJECT:
                    return immutableMemoAllocationsAvoided(value);
                default:
                    return 0;
            }
        }

        std::uint32_t initializerObjectAllocationsAvoided(const ImmutableObjectInitializer &initializer)
        {
            std::uint32_t result = 0;
            auto &typeManager = LangToolkit::getTypeManager();
            for (const auto &objectValue: initializer.objects()) {
                if (!objectValue.m_object) {
                    continue;
                }
                result = saturatedAdd(
                    result, nestedAllocationsAvoided(
                        typeManager.getTypeId(objectValue.m_object.get()), objectValue.m_object.get()
                    )
                );
            }
            return result;
        }

        std::uint32_t immutableMemoAllocationsAvoided(ObjectPtr value)
        {
            if (!LangToolkit::isMemoImmutableObject(value)) {
                return 0;
            }

            const auto &object = LangToolkit::getTypeManager().template extractObject<MemoImmutableObject>(value);
            if (object.hasInstance()) {
                return 0;
            }

            auto *initializer = dynamic_cast<ImmutableObjectInitializer *>(
                InitManager::instance.findInitializer(object)
            );
            if (!initializer) {
                return 0;
            }

            return saturatedAdd(1, initializerObjectAllocationsAvoided(*initializer));
        }

    }

    std::optional<EmbeddingMeasure> tryMeasureEmbeddingValue(
        TypeId typeId, StorageClass storageClass, ObjectPtr value
    )
    {
        if (!value) {
            return std::nullopt;
        }

        switch (storageClass) {
            case StorageClass::STRING_REF:
            case StorageClass::POOLED_STRING:
            case StorageClass::STR64:
                if (typeId != TypeId::STRING) {
                    return std::nullopt;
                }
                return makeMeasure(
                    storageClass, o_tuple_item::measure(stringElement(value)),
                    db0::o_string::measure(LangToolkit::getTypeManager().extractString(value)), 1
                );

            case StorageClass::DB0_BYTES:
            case StorageClass::DB0_BYTES_ARRAY:
                if (typeId != TypeId::BYTES && typeId != TypeId::BYTES_ARRAY) {
                    return std::nullopt;
                }
                {
                    auto bytes = LangToolkit::getTypeManager().extractBytes(value);
                    return makeMeasure(
                        storageClass, o_tuple_item::measure(bytesElement(value)),
                        db0::o_binary::measure(bytes.m_data, bytes.m_size), 1
                    );
                }

            case StorageClass::DB0_LIST:
            case StorageClass::DB0_TUPLE:
                if (typeId != TypeId::LIST && typeId != TypeId::TUPLE) {
                    return std::nullopt;
                }
                return makeMeasure(
                    storageClass, o_py_tuple::measure(value), 0, nestedAllocationsAvoided(typeId, value), false, true
                );

            case StorageClass::DB0_SET:
                if (typeId != TypeId::SET) {
                    return std::nullopt;
                }
                return makeMeasure(
                    storageClass, o_py_set::measure(value), 0, nestedAllocationsAvoided(typeId, value), false, true
                );

            case StorageClass::DB0_DICT:
                if (typeId != TypeId::DICT) {
                    return std::nullopt;
                }
                return makeMeasure(
                    storageClass, o_py_dict::measure(value), 0, nestedAllocationsAvoided(typeId, value), false, true
                );

            case StorageClass::OBJECT_REF: {
                if (typeId != TypeId::MEMO_IMMUTABLE_OBJECT || !LangToolkit::isMemoImmutableObject(value)) {
                    return std::nullopt;
                }

                const auto &object = LangToolkit::getTypeManager().template extractObject<MemoImmutableObject>(value);
                if (object.hasInstance()) {
                    return std::nullopt;
                }

                auto *initializer = dynamic_cast<ImmutableObjectInitializer *>(
                    InitManager::instance.findInitializer(object)
                );
                if (!initializer) {
                    return std::nullopt;
                }

                auto classRef = initializer->getClassPtr()->getClassRef();
                return makeMeasure(
                    storageClass, o_embedded_object::measure(classRef, *initializer),
                    0, saturatedAdd(1, initializerObjectAllocationsAvoided(*initializer)), true, false
                );
            }

            default:
                return std::nullopt;
        }
    }

    bool shouldEmbedValue(TypeId typeId, StorageClass storageClass, ObjectPtr value)
    {
        auto measure = tryMeasureEmbeddingValue(typeId, storageClass, value);
        if (!measure) {
            return false;
        }

        if (measure->m_separateStorageBytes == 0 && measure->m_allocationsAvoided == 0) {
            return false;
        }

        auto savedCost = measure->m_separateStorageBytes + measure->m_allocationsAvoided * ALLOCATION_COST;
        auto embeddedCost = measure->m_embeddedBytes
            + extraPagesFetched(measure->m_embeddedBytes) * PAGE_FETCH_COST;
        return savedCost > embeddedCost;
    }

}
