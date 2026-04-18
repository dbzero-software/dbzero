// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "WeakSet.hpp"
#include "WeakSetIterator.hpp"
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/bindings/python/PyWeakProxy.hpp>
#include <dbzero/bindings/python/MemoExpiredRef.hpp>
#include <dbzero/object_model/value.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/object.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/object_model/value/Member.hpp>
#include <object.h>

namespace db0::object_model

{

    namespace py = db0::python;
    GC0_Define(WeakSet)

    using TypeId = db0::bindings::TypeId;

    static bool isMemoLike(TypeId type_id)
    {
        return type_id == TypeId::MEMO_OBJECT
            || type_id == TypeId::MEMO_IMMUTABLE_OBJECT
            || type_id == TypeId::DB0_WEAK_PROXY
            || type_id == TypeId::MEMO_EXPIRED_REF;
    }

    // wrap a raw memo object as weak proxy and create a typed item with it
    static o_typed_item createWeakTypedItem(db0::swine_ptr<Fixture> &fixture,
        PyObject *lang_value, StorageClass storage_class, AccessFlags access_mode)
    {
        // wrap memo object as weak proxy (or pass-through if already a proxy)
        auto wrapped = db0::python::PyTypes::ObjectSharedPtr(
            db0::python::PyWeakProxy_Check(lang_value) ? Py_NewRef(lang_value) : db0::python::tryWeakProxy(lang_value),
            false);
        if (!wrapped) {
            THROWF(db0::InputException) << "Failed to construct weak proxy" << THROWF_END;
        }
        auto value = createMember<TypeId::DB0_WEAK_PROXY, db0::python::PyToolkit>(
            fixture, wrapped.get(), storage_class, access_mode);
        return { storage_class, value };
    }

    static set_item createWeakSetItem(db0::swine_ptr<Fixture> &fixture, std::uint64_t key,
        PyObject *lang_value, StorageClass storage_class, AccessFlags access_mode)
    {
        auto item = createWeakTypedItem(fixture, lang_value, storage_class, access_mode);
        SetIndex bindex(*fixture, item);
        return { key, bindex };
    }

    WeakSet::WeakSet(db0::swine_ptr<Fixture> &fixture, AccessFlags access_mode)
        : super_t(fixture, access_mode)
        , m_index(*fixture)
    {
        modify().m_index_ptr = m_index.getAddress();
    }

    WeakSet::WeakSet(tag_no_gc, db0::swine_ptr<Fixture> &fixture, const WeakSet &set)
        : super_t(tag_no_gc(), fixture)
        , m_index(*fixture)
    {
        modify().m_index_ptr = m_index.getAddress();
        std::uint64_t count = 0;
        for(auto [hash, address] : set) {
            auto bindex = address.getIndex(this->getMemspace());
            auto bindex_copy = SetIndex(bindex);
            m_index.insert(set_item(hash, bindex_copy));
            ++count;
        }
        modify().m_size = count;
    }

    WeakSet::WeakSet(db0::swine_ptr<Fixture> &fixture, Address address, AccessFlags access_mode)
        : super_t(super_t::tag_from_address(), fixture, address, access_mode)
        , m_index(myPtr((*this)->m_index_ptr))
    {
    }

    WeakSet::~WeakSet()
    {
        unregister();
    }

    void WeakSet::operator=(WeakSet &&other)
    {
        unrefMembers();
        super_t::operator=(std::move(other));
        m_index = std::move(other.m_index);
        assert(!other.hasInstance());
        restoreIterators();
    }

    void WeakSet::insert(const WeakSet &other)
    {
        for (auto [key, address] : other) {
            auto fixture = this->getFixture();
            auto bindex = address.getIndex(*fixture);
            auto it = bindex.beginJoin(1);
            while (!it.is_end()) {
                auto [storage_class, value] = (*it);
                auto member = unloadMember<LangToolkit>(fixture, storage_class, value, 0, getMemberFlags());
                // skip expired references when copying
                if (!db0::python::MemoExpiredRef_Check(member.get())) {
                    append(fixture, key, member.get());
                }
                ++it;
            }
        }
        restoreIterators();
    }

    void WeakSet::append(db0::FixtureLock &lock, std::size_t key, ObjectSharedPtr lang_value)
    {
        append(*lock, key, *lang_value);
        restoreIterators();
    }

    void WeakSet::append(db0::swine_ptr<Fixture> &fixture, std::size_t key, ObjectPtr lang_value)
    {
        auto type_id = LangToolkit::getTypeManager().getTypeId(lang_value);
        if (!isMemoLike(type_id)) {
            THROWF(db0::InputException) << "weak_set only accepts memo object instances" << THROWF_END;
        }

        // resolve short vs long weak ref based on target's prefix
        const auto &target_obj = LangToolkit::getTypeManager().extractAnyObject(lang_value);
        auto storage_class = (*target_obj.getFixture() != *fixture.get())
            ? StorageClass::OBJECT_LONG_WEAK_REF
            : StorageClass::OBJECT_WEAK_REF;

        auto iter = m_index.find(key);
        bool is_modified = false;
        if (iter == m_index.end()) {
            auto set_it = createWeakSetItem(fixture, key, lang_value, storage_class, getMemberFlags());
            m_index.insert(set_it);
            ++modify().m_size;
            is_modified = true;
        } else {
            // check if it's already there (by actual instance match)
            if (!hasItem(static_cast<std::int64_t>(key), lang_value)) {
                auto [it_key, address] = *iter;
                auto bindex = address.getIndex(*fixture);
                auto item = createWeakTypedItem(fixture, lang_value, storage_class, getMemberFlags());
                bindex.insert(item);
                if (bindex.getAddress() != address.m_index_address) {
                    m_index.erase(iter);
                    m_index.insert({key, bindex});
                }
                ++modify().m_size;
                is_modified = true;
            }
        }

        if (is_modified) {
            restoreIterators();
        }
    }

    bool WeakSet::remove(FixtureLock &, std::size_t key, ObjectPtr key_value)
    {
        auto iter = m_index.find(key);
        if (iter == m_index.end()) {
            return false;
        }
        auto [it_key, address] = *iter;
        auto bindex = address.getIndex(this->getMemspace());

        auto it = bindex.beginJoin(1);
        auto fixture = this->getFixture();
        while (!it.is_end()) {
            auto [storage_class, value] = *it;
            auto member = unloadMember<LangToolkit>(fixture, storage_class, value, 0, getMemberFlags());
            if (!db0::python::MemoExpiredRef_Check(member.get())
                && LangToolkit::compare(key_value, member.get())) {
                if (bindex.size() == 1) {
                    m_index.erase(iter);
                    bindex.destroy();
                } else {
                    bindex.erase(*it);
                }
                --modify().m_size;
                restoreIterators();
                return true;
            }
            ++it;
        }
        return false;
    }

    void WeakSet::destroy()
    {
        unrefMembers();
        m_index.destroy();
        super_t::destroy();
    }

    bool WeakSet::hasItem(std::int64_t hash_key, ObjectPtr key_value) const
    {
        auto iter = m_index.find(hash_key);
        if (iter == m_index.end()) {
            return false;
        }

        auto [key, address] = *iter;
        auto fixture = this->getFixture();
        auto bindex = address.getIndex(*fixture);
        auto it = bindex.beginJoin(1);
        while (!it.is_end()) {
            auto [storage_class, value] = *it;
            auto member = unloadMember<LangToolkit>(fixture, storage_class, value, 0, getMemberFlags());
            if (!db0::python::MemoExpiredRef_Check(member.get())
                && LangToolkit::compare(key_value, member.get())) {
                return true;
            }
            ++it;
        }
        return false;
    }

    void WeakSet::moveTo(db0::swine_ptr<Fixture> &fixture)
    {
        assert(hasInstance());
        if ((*this)->m_size > 0) {
            THROWF(db0::InputException) << "WeakSet with items cannot be moved to another fixture";
        }
        super_t::moveTo(fixture);
    }

    std::size_t WeakSet::size() const
    {
        // full scan - skip expired entries
        auto fixture = this->getFixture();
        std::size_t count = 0;
        for (auto [_, address] : m_index) {
            auto bindex = address.getIndex(this->getMemspace());
            auto it = bindex.beginJoin(1);
            while (!it.is_end()) {
                auto [storage_class, value] = *it;
                auto member = unloadMember<LangToolkit>(fixture, storage_class, value, 0, getMemberFlags());
                if (!db0::python::MemoExpiredRef_Check(member.get())) {
                    ++count;
                }
                ++it;
            }
        }
        return count;
    }

    void WeakSet::clear(FixtureLock &)
    {
        unrefMembers();
        m_index.clear();
        modify().m_size = 0;
        restoreIterators();
    }

    WeakSet::const_iterator WeakSet::begin() const {
        return m_index.begin();
    }

    WeakSet::const_iterator WeakSet::end() const {
        return m_index.end();
    }

    void WeakSet::commit() const
    {
        m_index.commit();
        super_t::commit();
    }

    void WeakSet::detach() const
    {
        commit();
        m_index.detach();
        m_iterators.forEach([](WeakSetIterator &iter) {
            iter.detach();
        });
        super_t::detach();
    }

    void WeakSet::unrefMembers() const
    {
        // Weak references do not increment ref-counts, so no unref needed for items.
        // For OBJECT_LONG_WEAK_REF, the LongWeakRef storage will be cleaned up when
        // the WeakSet's underlying memory is freed (m_index.destroy()).
    }

    std::shared_ptr<WeakSetIterator> WeakSet::getIterator(ObjectPtr lang_set) const
    {
        auto iter = std::shared_ptr<WeakSetIterator>(new WeakSetIterator(m_index.begin(), this, lang_set));
        m_iterators.push_back(iter);
        return iter;
    }

    void WeakSet::restoreIterators()
    {
        if (m_iterators.cleanup()) {
            return;
        }
        m_iterators.forEach([](WeakSetIterator &iter) {
            iter.restore();
        });
    }

    WeakSet::const_iterator WeakSet::find(std::uint64_t key_hash) const {
        return m_index.find(key_hash);
    }

}
