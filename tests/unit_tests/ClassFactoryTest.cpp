// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#include <gtest/gtest.h>
#include <utils/utils.hpp>
#include <mutex>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/workspace/PrefixName.hpp>
#include <dbzero/object_model/ObjectModel.hpp>
#include <dbzero/object_model/class.hpp>
#include <utils/SubClass.hpp>

using namespace std;
using namespace db0;
using namespace db0::object_model;
using namespace db0::tests;
    
namespace tests

{
    
    class ClassFactoryTest: public testing::Test
    {
    public:
        static constexpr const char *prefix_name = "my-test-prefix_1";
        static constexpr const char *file_name = "my-test-prefix_1.db0";

        void SetUp() override {
            drop(file_name);
        }

        void TearDown() override
        {            
            m_workspace.close();
            drop(file_name);
        }
        
    protected:
        Workspace m_workspace;        
    };
    
    TEST_F(ClassFactoryTest, InheritedIndexedMemberCacheTracksBaseIndexChanges)
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);
        auto base = std::make_shared<SubClass>(fixture, "IndexedBase", std::nullopt,
            "indexed.base", nullptr, std::vector<std::string>{}, ClassFlags(), nullptr);
        auto derived = std::make_shared<SubClass>(fixture, "IndexedDerived", std::nullopt,
            "indexed.derived", nullptr, std::vector<std::string>{}, ClassFlags(), base);
        auto leaf = std::make_shared<SubClass>(fixture, "IndexedLeaf", std::nullopt,
            "indexed.leaf", nullptr, std::vector<std::string>{}, ClassFlags(), derived);
        auto base_field = base->addField("value", 0, false).primary().first;
        auto unrelated_field = derived->addField("unrelated", 0, false).primary().first;
        auto derived_field = derived->addField("value", 0, false).primary().first;
        ASSERT_EQ(base_field, unrelated_field);
        ASSERT_NE(base_field, derived_field);
        auto leaf_decoy = leaf->addField("leaf_decoy", 0, false).primary().first;
        leaf->addField("unrelated", 0, false);
        auto leaf_field = leaf->addField("value", 0, false).primary().first;
        ASSERT_EQ(base_field, leaf_decoy);
        ASSERT_NE(derived_field, leaf_field);

        // Warm negative entries before the ancestor declaration changes.
        ASSERT_FALSE(derived->getMember(derived_field).m_field_options[FieldOptions::INDEXED_FIELD]);
        ASSERT_FALSE(derived->getMember(unrelated_field).m_field_options[FieldOptions::INDEXED_FIELD]);
        ASSERT_FALSE(leaf->getMember(leaf_field).m_field_options[FieldOptions::INDEXED_FIELD]);
        ASSERT_FALSE(leaf->getMember(leaf_decoy).m_field_options[FieldOptions::INDEXED_FIELD]);
        for (int iteration = 0; iteration < 2; ++iteration) {
            base->addIndexedField(base_field);
            EXPECT_TRUE(derived->getMember(derived_field).m_field_options[FieldOptions::INDEXED_FIELD]);
            EXPECT_FALSE(derived->getMember(unrelated_field).m_field_options[FieldOptions::INDEXED_FIELD]);
            EXPECT_EQ(derived->tryGetFieldIndex(derived_field), base->tryGetFieldIndex(base_field));
            EXPECT_EQ(derived->tryGetFieldIndex(unrelated_field), nullptr);
            auto leaf_member = leaf->getMember(leaf_field);
            EXPECT_TRUE(leaf_member.m_field_options[FieldOptions::INDEXED_FIELD]);
            EXPECT_EQ(leaf->tryGetFieldIndex(leaf_field), derived->tryGetFieldIndex(derived_field));
            EXPECT_FALSE(leaf->getMember(leaf_decoy).m_field_options[FieldOptions::INDEXED_FIELD]);
            EXPECT_EQ(leaf->tryGetFieldIndex(leaf_decoy), nullptr);
            base->removeIndexedField(base_field);
            EXPECT_FALSE(derived->getMember(derived_field).m_field_options[FieldOptions::INDEXED_FIELD]);
            EXPECT_EQ(derived->tryGetFieldIndex(derived_field), nullptr);
            EXPECT_FALSE(leaf->getMember(leaf_field).m_field_options[FieldOptions::INDEXED_FIELD]);
            EXPECT_EQ(leaf->tryGetFieldIndex(leaf_field), nullptr);
        }
        workspace.close();
    }

    TEST_F( ClassFactoryTest , testClassFactoryCanBeCreated )
    {        
        auto fixture = m_workspace.getFixture("my-test-prefix_1");
        ASSERT_NO_THROW( { auto cut = ClassFactory(fixture); } );
        m_workspace.close();
    }

}
