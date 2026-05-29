# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

from dataclasses import dataclass
from itertools import product
from typing import Optional

import dbzero as db0


@db0.memo(immutable=True, intern=True)
@dataclass
class Issue17PackedMask:
    create: Optional[bool] = None
    read: Optional[bool] = None
    update: Optional[bool] = None
    delete: Optional[bool] = None


def test_interned_immutable_object_with_only_pack_2_fields_materializes(db0_fixture):
    field_names = ("create", "read", "update", "delete")
    values = (None, False, True)
    seen_uuids = set()

    for combination in product(values, repeat=len(field_names)):
        kwargs = dict(zip(field_names, combination))

        materialized = db0.materialized(Issue17PackedMask(**kwargs))
        duplicate = db0.materialized(Issue17PackedMask(**kwargs))

        assert tuple(getattr(materialized, name) for name in field_names) == combination
        assert tuple(getattr(duplicate, name) for name in field_names) == combination
        assert db0.uuid(duplicate) == db0.uuid(materialized)
        seen_uuids.add(db0.uuid(materialized))

    assert len(seen_uuids) == len(values) ** len(field_names)
