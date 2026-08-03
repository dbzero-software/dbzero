# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

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


def test_as_tag_rejects_composite_tag_arguments(db0_fixture):
    user = CompositeTagUser("user-1")

    with pytest.raises(TypeError, match="exactly 1 argument"):
        db0.as_tag("GRANT-READ", user)


def test_as_tag_rejects_tuple_composite_tag_argument(db0_fixture):
    user = CompositeTagUser("user-1")

    with pytest.raises(TypeError, match="Expected a memo object"):
        db0.as_tag(("GRANT-READ", user))


def test_tuple_tag_argument_is_a_regular_tag_batch(db0_fixture):
    document = CompositeTagDocument("doc-1")

    db0.tags(document).add(("GRANT-READ", "active"))

    assert [doc.title for doc in db0.find("GRANT-READ")] == ["doc-1"]
    assert [doc.title for doc in db0.find("active")] == ["doc-1"]
    assert [doc.title for doc in db0.find(("GRANT-READ", "active"))] == ["doc-1"]
