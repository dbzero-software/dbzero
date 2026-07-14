// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "JoinIterator.hpp"
#include "JoinIterable.hpp"
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/core/memory/Address.hpp>

namespace db0::object_model

{

    JoinIterator::JoinIterator(db0::swine_ptr<Fixture> fixture, std::unique_ptr<TP_Iterator> &&query_iterator,
        const std::vector<std::shared_ptr<Class> > &types, const std::vector<TypeObjectSharedPtr> &lang_types,
        const std::vector<FilterFunc> &filters)
        : JoinIterable(std::move(fixture), std::move(query_iterator), types, lang_types, filters)
        , m_query_iterator(m_iterator->begin())
    {
    }

    JoinIterator::ObjectSharedPtr JoinIterator::next()
    {
        TP_Vector<db0::UniqueAddress> next_key;
        for (;;) {
            if (!m_query_iterator->isEnd()) {
                m_query_iterator->next(&next_key);                
                auto tuple_ptr = unloadTuple(next_key);
                // check filters if any
                for (auto &filter: m_filters) {
                    if (!filter(LangToolkit::unpackTuple(tuple_ptr.get()))) {
                        tuple_ptr = nullptr;
                        break;
                    }
                }
                if (tuple_ptr.get()) {
                    return tuple_ptr;
                }
            } else {
                return nullptr;
            }
        }
    }
    
    JoinIterator::ObjectSharedPtr JoinIterator::unloadTuple(const TP_Vector<UniqueAddress> &addrs) const
    {
        auto fixture = getFixture();
        std::vector<ObjectSharedPtr> tuple_objs(addrs.size());
        auto type = m_types.begin();
        auto lang_type = m_lang_types.begin();
        for (std::size_t i = 0; i < addrs.size(); ++i) {
            if (*type) {
                tuple_objs[i] = LangToolkit::unloadObject(
                    fixture, addrs[i].getAddress(), *type, lang_type->get()
                );
            } else {
                tuple_objs[i] = LangToolkit::unloadObject(
                    fixture, addrs[i].getAddress(), m_class_factory, lang_type->get()
                );
            }            
            ++type;
            ++lang_type;
        }
        return LangToolkit::makeTuple(std::move(tuple_objs));
    }

}
