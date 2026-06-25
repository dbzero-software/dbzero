# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2026 DBZero Software sp. z o.o.

import dbzero as db0
import pytest


@db0.memo
class CompositeTagUser:
    def __init__(self, name):
        self.name = name


@db0.memo
class CompositeTagDocument:
    def __init__(self, title):
        self.title = title


def test_can_add_tuple_based_composite_tag(db0_fixture):
    user = CompositeTagUser("user-1")
    document = CompositeTagDocument("doc-1")

    db0.tags(document).add(("GRANT-READ", user))
    db0.commit()


def test_can_add_as_tag_composite_tag(db0_fixture):
    user = CompositeTagUser("user-1")
    document = CompositeTagDocument("doc-1")

    db0.tags(document).add(db0.as_tag("GRANT-READ", user))
    db0.commit()


def test_can_add_as_tag_tuple_composite_tag(db0_fixture):
    user = CompositeTagUser("user-1")
    document = CompositeTagDocument("doc-1")

    db0.tags(document).add(db0.as_tag(("GRANT-READ", user)))
    db0.commit()


def test_can_remove_as_tag_composite_tag(db0_fixture):
    user = CompositeTagUser("user-1")
    document = CompositeTagDocument("doc-1")
    composite_tag = db0.as_tag("GRANT-READ", user)

    db0.tags(document).add(composite_tag)
    db0.tags(document).remove(composite_tag)
    db0.commit()


def test_find_by_as_tag_composite_tag(db0_fixture):
    user_1 = CompositeTagUser("user-1")
    user_2 = CompositeTagUser("user-2")
    document_1 = CompositeTagDocument("doc-1")
    document_2 = CompositeTagDocument("doc-2")
    document_3 = CompositeTagDocument("doc-3")

    db0.tags(document_1).add(db0.as_tag("GRANT-READ", user_1))
    db0.tags(document_2).add(db0.as_tag("GRANT-READ", user_2))
    db0.tags(document_3).add("GRANT-READ")

    assert [doc.title for doc in db0.find(db0.as_tag("GRANT-READ", user_1))] == ["doc-1"]
    assert [doc.title for doc in db0.find(db0.as_tag(("GRANT-READ", user_2)))] == ["doc-2"]


def test_find_by_composite_tag_with_type_filter(db0_fixture):
    user = CompositeTagUser("user-1")
    document = CompositeTagDocument("doc-1")
    other = CompositeTagUser("not-a-document")

    db0.tags(document).add(db0.as_tag("GRANT-READ", user))
    db0.tags(other).add(db0.as_tag("GRANT-READ", user))

    assert [doc.title for doc in db0.find(CompositeTagDocument, db0.as_tag("GRANT-READ", user))] == ["doc-1"]


def test_find_by_composite_tag_combines_with_simple_tags(db0_fixture):
    user = CompositeTagUser("user-1")
    document_1 = CompositeTagDocument("doc-1")
    document_2 = CompositeTagDocument("doc-2")

    db0.tags(document_1).add(db0.as_tag("GRANT-READ", user), "active")
    db0.tags(document_2).add(db0.as_tag("GRANT-READ", user), "archived")

    assert [doc.title for doc in db0.find(db0.as_tag("GRANT-READ", user), "active")] == ["doc-1"]


def test_find_by_missing_composite_leaf_returns_empty(db0_fixture):
    user_1 = CompositeTagUser("user-1")
    user_2 = CompositeTagUser("user-2")
    document = CompositeTagDocument("doc-1")

    db0.tags(document).add(db0.as_tag("GRANT-READ", user_1))

    assert list(db0.find(db0.as_tag("GRANT-READ", user_2))) == []


def test_find_by_composite_tag_inside_or_clause(db0_fixture):
    user = CompositeTagUser("user-1")
    document_1 = CompositeTagDocument("doc-1")
    document_2 = CompositeTagDocument("doc-2")

    db0.tags(document_1).add(db0.as_tag("GRANT-READ", user))
    db0.tags(document_2).add("fallback")

    assert {doc.title for doc in db0.find([db0.as_tag("GRANT-READ", user), "fallback"])} == {"doc-1", "doc-2"}


def test_find_by_composite_tag_with_negation(db0_fixture):
    user = CompositeTagUser("user-1")
    document_1 = CompositeTagDocument("doc-1")
    document_2 = CompositeTagDocument("doc-2")

    db0.tags(document_1).add(db0.as_tag("GRANT-READ", user))
    db0.tags(document_2).add("visible")

    assert [doc.title for doc in db0.find(CompositeTagDocument, db0.no(db0.as_tag("GRANT-READ", user)))] == ["doc-2"]


def test_composite_tags_with_many_objects_and_many_tags(db0_fixture):
    users = [CompositeTagUser(f"user-{i}") for i in range(12)]
    documents = [CompositeTagDocument(f"doc-{i}") for i in range(60)]
    predicates = [f"perm-{i}" for i in range(15)]
    removed_pairs = set()

    def pairs_for_document(index):
        return {
            (index % len(predicates), index % len(users)),
            ((index + 1) % len(predicates), (index + 3) % len(users)),
        }

    def titles_for(predicate_index, user_index):
        return {
            documents[index].title
            for index in range(len(documents))
            if (predicate_index, user_index) in pairs_for_document(index)
            and (index, predicate_index, user_index) not in removed_pairs
        }

    def query_titles(*args):
        return {doc.title for doc in db0.find(*args)}

    for index, document in enumerate(documents):
        composite_tags = [
            db0.as_tag(predicates[predicate_index], users[user_index])
            for predicate_index, user_index in sorted(pairs_for_document(index))
        ]
        simple_tags = ["active" if index % 2 == 0 else "archived"]
        if index % 3 == 0:
            simple_tags.append("reviewed")
        db0.tags(document).add(*composite_tags, *simple_tags)

    assert query_titles(db0.as_tag(predicates[4], users[4])) == titles_for(4, 4)
    assert query_titles(db0.as_tag((predicates[7], users[10]))) == titles_for(7, 10)

    active_expected = {
        title
        for title in titles_for(4, 4)
        if int(title.split("-")[1]) % 2 == 0
    }
    assert query_titles(CompositeTagDocument, db0.as_tag(predicates[4], users[4]), "active") == active_expected

    reviewed_expected = {document.title for index, document in enumerate(documents) if index % 3 == 0}
    assert query_titles([db0.as_tag(predicates[7], users[10]), "reviewed"]) == (
        titles_for(7, 10) | reviewed_expected
    )

    denied_expected = {document.title for document in documents} - titles_for(4, 4)
    assert query_titles(CompositeTagDocument, db0.no(db0.as_tag(predicates[4], users[4]))) == denied_expected

    for index in range(0, len(documents), 5):
        predicate_index, user_index = next(iter(pairs_for_document(index)))
        db0.tags(documents[index]).remove(db0.as_tag(predicates[predicate_index], users[user_index]))
        removed_pairs.add((index, predicate_index, user_index))

    assert query_titles(db0.as_tag(predicates[0], users[0])) == titles_for(0, 0)
    assert query_titles(db0.as_tag(predicates[5], users[5])) == titles_for(5, 5)
    assert query_titles(CompositeTagDocument, db0.as_tag(predicates[10], users[10])) == titles_for(10, 10)


def test_rejects_nested_composite_tag_before_update(db0_fixture):
    user = CompositeTagUser("user-1")
    document = CompositeTagDocument("doc-1")

    with pytest.raises(Exception):
        db0.tags(document).add("simple-tag", ("GRANT-READ", ("nested", user)))

    db0.tags(document).add(("GRANT-READ", user))
    db0.commit()


def test_rejects_nested_composite_tag_on_remove(db0_fixture):
    user = CompositeTagUser("user-1")
    document = CompositeTagDocument("doc-1")

    db0.tags(document).add(("GRANT-READ", user))

    with pytest.raises(Exception):
        db0.tags(document).remove("simple-tag", ("GRANT-READ", ("nested", user)))

    db0.commit()
