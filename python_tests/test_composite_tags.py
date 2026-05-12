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
