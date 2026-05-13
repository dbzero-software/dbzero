// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "FieldIDMapper.hpp"
#include <cassert>
#include <cstring>

namespace db0::object_model

{

    FieldIDMapper::FieldIDMapper(db0::Memspace &memspace)
        : super_t(memspace)
    {
    }

    FieldIDMapper::FieldIDMapper(db0::mptr ptr)
        : super_t(ptr)
    {
    }

    FieldClusterOffsetMap *FieldIDMapper::getFieldClusterOffsets() const
    {
        if ((*this)->m_field_cluster_offsets_ptr.isNull()) {
            return nullptr;
        }
        if (m_field_cluster_offsets.isNull()) {
            m_field_cluster_offsets = (*this)->m_field_cluster_offsets_ptr(this->getMemspace());
        }
        return &m_field_cluster_offsets;
    }

    FieldClusterOffsetMap &FieldIDMapper::getOrCreateFieldClusterOffsets()
    {
        if (m_field_cluster_offsets.isNull()) {
            auto &ptr = this->modify().m_field_cluster_offsets_ptr;
            if (ptr.isNull()) {
                m_field_cluster_offsets = ptr.create(this->getMemspace());
            } else {
                m_field_cluster_offsets = ptr(this->getMemspace());
            }
        }
        return m_field_cluster_offsets;
    }

    FieldExceptionOffsetMap *FieldIDMapper::getFieldExceptionOffsets() const
    {
        if ((*this)->m_field_exception_offsets_ptr.isNull()) {
            return nullptr;
        }
        if (m_field_exception_offsets.isNull()) {
            m_field_exception_offsets = (*this)->m_field_exception_offsets_ptr(this->getMemspace());
        }
        return &m_field_exception_offsets;
    }

    FieldExceptionOffsetMap &FieldIDMapper::getOrCreateFieldExceptionOffsets()
    {
        if (m_field_exception_offsets.isNull()) {
            auto &ptr = this->modify().m_field_exception_offsets_ptr;
            if (ptr.isNull()) {
                m_field_exception_offsets = ptr.create(this->getMemspace());
            } else {
                m_field_exception_offsets = ptr(this->getMemspace());
            }
        }
        return m_field_exception_offsets;
    }

    NameOffsetMap *FieldIDMapper::getNameOffsets() const
    {
        if ((*this)->m_name_offsets_ptr.isNull()) {
            return nullptr;
        }
        if (m_name_offsets.isNull()) {
            m_name_offsets = (*this)->m_name_offsets_ptr(this->getMemspace());
        }
        return &m_name_offsets;
    }

    NameOffsetMap &FieldIDMapper::getOrCreateNameOffsets()
    {
        if (m_name_offsets.isNull()) {
            auto &ptr = this->modify().m_name_offsets_ptr;
            if (ptr.isNull()) {
                m_name_offsets = ptr.create(this->getMemspace());
            } else {
                m_name_offsets = ptr(this->getMemspace());
            }
        }
        return m_name_offsets;
    }

    std::optional<std::uint32_t> FieldIDMapper::tryGetFieldClusterOffset(std::uint32_t cluster_id) const
    {
        if (cluster_id == 0) {
            return 0;
        }

        auto cached = m_field_cluster_offset_cache.find(cluster_id);
        if (cached != m_field_cluster_offset_cache.end()) {
            return cached->second;
        }

        auto field_cluster_offsets = getFieldClusterOffsets();
        if (!field_cluster_offsets) {
            return std::nullopt;
        }

        auto it = field_cluster_offsets->find(cluster_id);
        if (it == field_cluster_offsets->end()) {
            return std::nullopt;
        }

        auto cluster_offset = (*it).value;
        m_field_cluster_offset_cache[cluster_id] = cluster_offset;
        return cluster_offset;
    }

    std::optional<std::uint32_t> FieldIDMapper::tryGetFieldExceptionOffset(FieldID field_id) const
    {
        auto key = field_id.getLongIndex();
        auto cached = m_field_exception_offset_cache.find(key);
        if (cached != m_field_exception_offset_cache.end()) {
            return cached->second;
        }

        auto field_exception_offsets = getFieldExceptionOffsets();
        if (!field_exception_offsets) {
            return std::nullopt;
        }

        auto it = field_exception_offsets->find(key);
        if (it == field_exception_offsets->end()) {
            return std::nullopt;
        }

        auto offset = (*it).value;
        m_field_exception_offset_cache[key] = offset;
        return offset;
    }

    std::optional<std::uint32_t> FieldIDMapper::tryGetNameOffset(const char *field_name) const
    {
        auto cached = m_name_offset_cache.find(field_name);
        if (cached != m_name_offset_cache.end()) {
            return cached->second;
        }

        auto name_offsets = getNameOffsets();
        if (!name_offsets) {
            return std::nullopt;
        }

        auto it = name_offsets->find(field_name);
        if (it == name_offsets->end()) {
            return std::nullopt;
        }

        auto offset = static_cast<std::uint32_t>(it->second());
        m_name_offset_cache[field_name] = offset;
        return offset;
    }

    namespace
    {
        std::uint32_t alignClusterOffset(std::uint32_t offset)
        {
            auto remainder = offset % FieldIDMapper::CLUSTER_SIZE;
            if (remainder) {
                offset += FieldIDMapper::CLUSTER_SIZE - remainder;
            }
            return offset;
        }
    }

    std::uint32_t FieldIDMapper::assignNextNameOffset()
    {
        auto offset = (*this)->m_next_name_offset;
        if (offset < CLUSTER_SIZE) {
            offset = CLUSTER_SIZE;
        }
        if (offset % CLUSTER_SIZE == 0 && offset < (*this)->m_next_cluster_offset) {
            offset = (*this)->m_next_cluster_offset;
        }
        this->modify().m_next_name_offset = offset + 1;
        return offset;
    }

    std::uint32_t FieldIDMapper::assignNextClusterOffset()
    {
        auto offset = (*this)->m_next_cluster_offset;
        if (offset < CLUSTER_SIZE) {
            offset = CLUSTER_SIZE;
        }

        auto next_name_cluster_boundary = alignClusterOffset((*this)->m_next_name_offset);
        if (offset < next_name_cluster_boundary) {
            offset = next_name_cluster_boundary;
        }

        this->modify().m_next_cluster_offset = offset + CLUSTER_SIZE;
        return offset;
    }

    std::uint32_t FieldIDMapper::assignFieldOffset(const char *field_name)
    {
        assert(field_name);
        auto maybe_offset = tryGetNameOffset(field_name);
        if (maybe_offset) {
            recordFieldOffsetRange(*maybe_offset);
            return *maybe_offset;
        }

        auto offset = assignNextNameOffset();
        auto &name_offsets = getOrCreateNameOffsets();
        auto [it, inserted] = name_offsets.insert_unique(field_name, offset);
        if (!inserted) {
            offset = static_cast<std::uint32_t>(it->second());
        }

        m_name_offset_cache[field_name] = offset;
        recordFieldOffsetRange(offset);
        return offset;
    }

    bool FieldIDMapper::renameField(const char *old_field_name, const char *new_field_name)
    {
        assert(old_field_name);
        assert(new_field_name);

        auto maybe_offset = tryGetNameOffset(old_field_name);
        if (!maybe_offset) {
            return false;
        }

        if (std::strcmp(old_field_name, new_field_name) == 0) {
            return true;
        }

        if (tryGetNameOffset(new_field_name)) {
            return false;
        }

        auto name_offsets = getNameOffsets();
        assert(name_offsets);

        auto it = name_offsets->find(old_field_name);
        if (it == name_offsets->end()) {
            m_name_offset_cache.erase(old_field_name);
            return false;
        }

        auto offset = *maybe_offset;
        name_offsets->erase(it);
        auto [new_it, inserted] = name_offsets->insert_unique(new_field_name, offset);
        if (!inserted) {
            m_name_offset_cache.erase(old_field_name);
            m_name_offset_cache[new_field_name] = static_cast<std::uint32_t>(new_it->second());
            return false;
        }

        m_name_offset_cache.erase(old_field_name);
        m_name_offset_cache[new_field_name] = offset;
        return true;
    }

    std::optional<std::uint32_t> FieldIDMapper::tryGetAssignedFieldOffset(FieldID field_id) const
    {
        auto maybe_exception_offset = tryGetFieldExceptionOffset(field_id);
        if (maybe_exception_offset) {
            return *maybe_exception_offset;
        }

        auto maybe_cluster_offset = tryGetFieldClusterOffset(field_id.getIndex());
        if (maybe_cluster_offset) {
            return *maybe_cluster_offset + field_id.getOffset();
        }

        return std::nullopt;
    }

    std::uint32_t FieldIDMapper::assignFieldOffset(FieldID field_id)
    {
        auto maybe_offset = tryGetAssignedFieldOffset(field_id);
        if (maybe_offset) {
            recordFieldOffsetRange(*maybe_offset);
            return *maybe_offset;
        }

        auto cluster_offset = assignNextClusterOffset();
        auto &field_cluster_offsets = getOrCreateFieldClusterOffsets();
        auto it = field_cluster_offsets.find(field_id.getIndex());
        if (it != field_cluster_offsets.end()) {
            cluster_offset = (*it).value;
        } else {
            field_cluster_offsets.insert({ field_id.getIndex(), cluster_offset });
        }

        m_field_cluster_offset_cache[field_id.getIndex()] = cluster_offset;
        auto offset = cluster_offset + field_id.getOffset();
        recordFieldOffsetRange(offset);
        return offset;
    }

    std::uint32_t FieldIDMapper::assignFieldExceptionOffset(FieldID field_id, std::uint32_t offset)
    {
        auto &field_exception_offsets = getOrCreateFieldExceptionOffsets();
        auto it = field_exception_offsets.find(field_id.getLongIndex());
        if (it != field_exception_offsets.end()) {
            offset = (*it).value;
        } else {
            field_exception_offsets.insert({ field_id.getLongIndex(), offset });
        }

        m_field_exception_offset_cache[field_id.getLongIndex()] = offset;
        recordFieldOffsetRange(offset);
        return offset;
    }

    std::uint32_t FieldIDMapper::onFieldIDAssigned(const char *field_name, FieldID field_id)
    {
        assert(field_name);
        auto maybe_offset = tryGetNameOffset(field_name);
        if (!maybe_offset) {
            auto maybe_field_offset = tryGetAssignedFieldOffset(field_id);
            if (maybe_field_offset) {
                recordFieldOffsetRange(*maybe_field_offset);
                return *maybe_field_offset;
            }
            return assignFieldExceptionOffset(field_id, assignNextNameOffset());
        }

        auto offset = *maybe_offset;
        auto name_offsets = getNameOffsets();
        if (name_offsets) {
            auto it = name_offsets->find(field_name);
            if (it != name_offsets->end()) {
                name_offsets->erase(it);
            }
        }

        m_name_offset_cache.erase(field_name);

        auto maybe_exception_offset = tryGetFieldExceptionOffset(field_id);
        if (maybe_exception_offset) {
            recordFieldOffsetRange(*maybe_exception_offset);
            return *maybe_exception_offset;
        }

        return assignFieldExceptionOffset(field_id, offset);
    }

    std::unordered_map<std::string, std::uint32_t> FieldIDMapper::getAssignedNameOffsets() const
    {
        std::unordered_map<std::string, std::uint32_t> result;
        auto name_offsets = getNameOffsets();
        if (!name_offsets) {
            return result;
        }

        for (auto it = name_offsets->begin(); it != name_offsets->end(); ++it) {
            auto field_name = it->first().toString();
            auto offset = static_cast<std::uint32_t>(it->second());
            result[field_name] = offset;
            m_name_offset_cache[field_name] = offset;
        }
        return result;
    }

    std::uint32_t FieldIDMapper::getFieldOffsetRange() const
    {
        return (*this)->m_field_offset_range;
    }

    void FieldIDMapper::recordFieldOffsetRange(std::uint32_t offset)
    {
        if (offset > (*this)->m_field_offset_range) {
            this->modify().m_field_offset_range = offset;
        }
    }

    std::uint32_t FieldIDMapper::getFieldIDMappingCount() const
    {
        return getFieldIDClusterMappingCount() + getFieldIDExceptionMappingCount();
    }

    std::uint32_t FieldIDMapper::getFieldIDClusterMappingCount() const
    {
        auto field_cluster_offsets = getFieldClusterOffsets();
        return field_cluster_offsets ? field_cluster_offsets->size() : 0;
    }

    std::uint32_t FieldIDMapper::getFieldIDExceptionMappingCount() const
    {
        auto field_exception_offsets = getFieldExceptionOffsets();
        return field_exception_offsets ? field_exception_offsets->size() : 0;
    }

    std::uint32_t FieldIDMapper::getNameMappingCount() const
    {
        auto name_offsets = getNameOffsets();
        return name_offsets ? name_offsets->size() : 0;
    }

    bool FieldIDMapper::hasFieldIDClusterMappingCollection() const
    {
        return !(*this)->m_field_cluster_offsets_ptr.isNull();
    }

    bool FieldIDMapper::hasFieldIDExceptionMappingCollection() const
    {
        return !(*this)->m_field_exception_offsets_ptr.isNull();
    }

    bool FieldIDMapper::hasNameMappingCollection() const
    {
        return !(*this)->m_name_offsets_ptr.isNull();
    }

    void FieldIDMapper::detach() const
    {
        if (!m_field_cluster_offsets.isNull()) {
            m_field_cluster_offsets.detach();
        }
        if (!m_field_exception_offsets.isNull()) {
            m_field_exception_offsets.detach();
        }
        if (!m_name_offsets.isNull()) {
            m_name_offsets.detach();
        }
        super_t::detach();
    }

    void FieldIDMapper::commit() const
    {
        if (!m_field_cluster_offsets.isNull()) {
            m_field_cluster_offsets.commit();
        }
        if (!m_field_exception_offsets.isNull()) {
            m_field_exception_offsets.commit();
        }
        if (!m_name_offsets.isNull()) {
            m_name_offsets.commit();
        }
        super_t::commit();
    }

}
