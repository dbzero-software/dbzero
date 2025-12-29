// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <dbzero/core/collections/rle/RLE_Sequence.hpp>

namespace tests

{
    
    TEST( RLE_SequenceTest , testRLE_SequenceCanCountConsecutiveIntegers )
    {
        using namespace db0;
        RLE_SequenceBuilder<unsigned int> cut;
        unsigned int value = 0;
        while (value < 50) {
            cut.append(value);
            ++value;
        }
        ++value;
        while (value < 100) {
            cut.append(value);
            ++value;
        }
        std::vector<char> buffer;
        auto &rle_sequence = cut.build(buffer);
        ASSERT_EQ(2u, rle_sequence.size());
    }

    TEST( RLE_SequenceTest , testRLE_SequenceCanBeDecodedWithIterator )
    {
        using namespace db0;
        std::vector<char> buffer;
        RLE_SequenceBuilder<unsigned int> builder;
        unsigned int value = 0;
        std::vector<unsigned int> in_values;
        while (value < 50) {
            builder.append(value);
            in_values.push_back(value);
            ++value;
        }
        ++value;
        while (value < 100) {
            builder.append(value);
            in_values.push_back(value);
            ++value;
        }

        auto &cut = builder.build(buffer);
        std::vector<unsigned int> out_values;
        for (auto value: cut) {
            out_values.push_back(value);
        }
        ASSERT_EQ(in_values, out_values);
    }

    TEST( RLE_SequenceTest , testEmptyRLE_SequenceCanCreatedAndIteratedOver )
    {
        using namespace db0;
        std::vector<char> buffer;
        RLE_SequenceBuilder<unsigned int> builder;
        // build empty sequence
        auto &cut = builder.build(buffer);        
        unsigned int count = 0;
        for (auto it = cut.begin(); it != cut.end(); ++it) {
            ++count;
        }
    }

    TEST( RLE_SequenceTest , testRLE_SequenceBuilderWillAddDuplicatesByDefault )
    {
        using namespace db0;
        std::vector<char> buffer;
        RLE_SequenceBuilder<unsigned int> cut;

        cut.append(1);
        cut.append(2);
        cut.append(2);
        cut.append(3);
        cut.append(3);
        cut.append(3);
        auto &result = cut.build(buffer);
        std::vector<unsigned int> expected_data { 1, 2, 2, 3, 3, 3 };
        std::vector<unsigned int> data;
        for (auto value: result) {
            data.push_back(value);
        }
        ASSERT_EQ(expected_data, data);
    }

    TEST( RLE_SequenceTest , testRLE_SequenceBuilderCanRemoveDuplicatesOnRequest )
    {
        using namespace db0;
        std::vector<char> buffer;
        RLE_SequenceBuilder<unsigned int> cut;

        cut.append(1, false);
        cut.append(2, false);
        cut.append(2, false);
        cut.append(3, false);
        cut.append(3, false);
        cut.append(3, false);
        cut.append(8, false);
        cut.append(8, false);
        auto &result = cut.build(buffer);
        std::vector<unsigned int> expected_data { 1, 2, 3, 8 };
        std::vector<unsigned int> data;
        for (auto value: result) {
            data.push_back(value);
        }
        ASSERT_EQ(expected_data, data);
    }
    
    TEST( RLE_SequenceTest , testRLE_SequenceCanHoldUnsortedItems )
    {
        using namespace db0;
        std::vector<char> buffer;
        RLE_SequenceBuilder<unsigned int> cut;

        cut.append(123, false);
        cut.append(2, false);
        cut.append(2, false);
        cut.append(3, false);
        cut.append(3, false);
        cut.append(3, false);
        cut.append(8, false);
        cut.append(8, false);
        auto &result = cut.build(buffer);
        std::vector<unsigned int> expected_data { 123, 2, 3, 8 };
        std::vector<unsigned int> data;
        for (auto value: result) {
            data.push_back(value);
        }
        ASSERT_EQ(expected_data, data);
    }

    TEST( RLE_SequenceTest , testRLE_SequenceCanRemoveDuplicatesOnPerItemBasis )
    {
        using namespace db0;
        std::vector<char> buffer;
        RLE_SequenceBuilder<unsigned int> cut;

        cut.append(2, true);
        cut.append(2, true);
        cut.append(2, false);
        cut.append(3, false);
        cut.append(3, false);
        cut.append(3, false);
        cut.append(8, false);
        cut.append(8, false);
        auto &result = cut.build(buffer);
        std::vector<unsigned int> expected_data { 2, 2, 3, 8 };
        std::vector<unsigned int> data;
        for (auto value: result) {
            data.push_back(value);
        }
        ASSERT_EQ(expected_data, data);
    }

}
