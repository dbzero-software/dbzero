#pragma once

#include <vector>
#include "FT_Iterator.hpp"
#include "SortedIterator.hpp"
#include <dbzero/workspace/Snapshot.hpp>

namespace db0::serial

{

    template <typename KeyT> std::unique_ptr<db0::FT_Iterator<KeyT> > deserializeFT_Iterator(
        db0::Snapshot &snapshot, std::vector<std::byte>::const_iterator &iter,
        std::vector<std::byte>::const_iterator end);

    template <typename KeyT> std::unique_ptr<db0::SortedIterator<KeyT> > deserializeSortedIterator(
        db0::Snapshot &snapshot, std::vector<std::byte>::const_iterator &iter,
        std::vector<std::byte>::const_iterator end);

}
