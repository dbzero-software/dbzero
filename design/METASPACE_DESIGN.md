# MetaSpace Design

This document describes the planned `MetaPrefix` / `MetaSpace` storage model
for dbzero metadata pages, including the multi-slot extension and integration
with `BDevStorage`.

## Goal

`MetaPrefix` stores durable metadata, with durable-page mapping metadata being
the primary use case. It is responsible for capturing and persisting only the
most recent head state of metadata pages. It is not intended to retain a full
history of metadata states.

The design builds on the existing in-memory `DRAM_Prefix` and
`DRAM_Allocator` machinery for data-page management and bookkeeping, but it
changes the persistence layer:

- Page contents are stored through a `Diff_IO` backed store.
- Logical-page-to-storage-location mappings are stored in an additional sparse
  pair managed outside the `MetaPrefix`.
- Updates prefer sequential diff-stream appends over random full-page
  overwrites.
- Periodic compaction rewrites head pages as full pages, clears old diffs, and
  bounds replay cost.

## Terminology

The design uses these terms:

- `DP`: a durable data page.
- `head state`: the newest committed state that must be reopened after restart.
- `historical state`: the previous committed state retained for crash safety.
- `full DP`: a complete page image stored at a specific `Diff_IO` location.
- `diff block`: an append-only delta against a previous full DP or diff chain.
- `DiffIndex`: the in-memory or durable index that tracks appended diff blocks.
- `sparse pair`: the external mapping from logical page id to storage location
  and diff sequence.
- `slot`: an independently managed metadata address-space partition in the
  multi-slot extension.

## Storage Model

`MetaPrefix` keeps the same in-memory page-management responsibilities as a
`DRAM_Prefix`. Allocated metadata pages have local logical page ids and are
managed by a `DRAM_Allocator`.

Persistent page locations are not stored directly inside the `MetaPrefix`.
Instead, `MetaPrefix` requires an additional sparse pair whose values describe
the current storage chain for each logical page:

```text
local logical page id -> full DP location + ordered diff locations
```

The sparse pair is maintained elsewhere so that the `MetaPrefix` can be used as
a metadata host without recursively depending on itself for its own location
mapping. For the multi-slot `MetaSpace` used by `BDevStorage`, this sparse pair
is maintained by the root-level `DRAM_Prefix`.

## Diff_IO

`Diff_IO` is the persistent store used by `MetaPrefix`. In production it is
typically embedded as a separate page-IO channel in the underlying
`BDevStorage`.

Required operations:

- Read a full DP from a specific location.
- Write a full DP to a specific location.
- Update or overwrite a full DP at a specific location.
- Append a diff block to the diff stream.
- Apply or replay diffs from a specific location or chain.
- Clear the diff stream so the space can be reused after compaction.

The implementation should treat full-page writes and diff appends differently.
Full-page writes are used for initial materialization, crash-safe state
rotation, and compaction. Ordinary metadata updates should generally be
persisted as appended diffs.

## Persistence Semantics

`MetaPrefix` persists only the head state, plus one previous historical state
needed for crash recovery. Retaining one historical state protects against a
crash that happens after part of the new head state has been persisted but
before all metadata needed to reopen it has become durable.

This implies a two-generation storage discipline:

- The current head generation is the generation reopened during normal startup.
- The previous generation is retained until the next head generation is fully
  durable.
- Full DP locations from older generations may be reused after they are no
  longer needed for crash recovery.

The sparse pair update must be ordered so that recovery can always choose a
complete generation. A crash must not expose a sparse-pair entry that points to
a partially written full DP or an incomplete diff sequence as the only
available state.

## Flush Mode

The default flush path should prefer appending diff blocks over overwriting full
DP locations.

Sequential appends are preferred because:

- They are usually faster than random writes.
- They match SSD write behavior better than repeatedly overwriting the same
  physical locations.
- They reduce premature cell wear caused by hot random overwrite patterns.
- They allow commits to persist small metadata changes without rewriting entire
  pages.

Full DP overwrites remain necessary for compaction, initial page creation, and
state-generation management, but they should not be the common path for small
metadata mutations.

## Diff Growth And Compaction

The diff stream must not grow without bound. Long diff chains increase startup
or page-load replay time and place unnecessary pressure on the `DiffIndex`.

Compaction is the administrative operation that bounds this cost:

1. Materialize every dirty or live head DP as a full DP.
2. Update the sparse pair so each logical page points to the new full-page
   location without old diff chains.
3. Ensure the new head generation is durable.
4. Retain the previous generation until it is safe to reclaim.
5. Clear the diff stream for reuse.
6. Clear or rebuild the `DiffIndex`.

Compaction may extend commit latency because it rewrites all head metadata
pages that need a compact full representation. The runtime should expose a
programmatic mechanism to suspend or postpone compaction when the system is
under load. While compaction is suspended, ordinary diff appends may continue
until the configured diff-stream cap forces the system to either resume
compaction or reject further growth with a clear operational error.

## Crash Consistency Invariants

The implementation must preserve these invariants:

- Startup can always recover either the latest complete head state or the
  previous complete historical state.
- A sparse-pair entry published as part of the head generation never points to
  storage that was not fully written.
- Diff replay for a page is ordered and deterministic.
- Clearing the diff stream only happens after all head DPs have full-page
  representations and the sparse pair no longer needs the old diff locations.
- Reusing full DP locations from old generations only happens after the
  previous generation is no longer needed for crash recovery.
- Compaction is atomic at the `MetaSpace` level, not per page.

## Multi-Slot MetaSpace

Multi-slot `MetaSpace` extends the regular `MetaSpace` model with independently
managed slots. A slot is a separate metadata address space with its own memory
mapping lifecycle. The term `slot` matches the allocator interface, although
the concept is closer to a realm.

Slots improve memory management by allowing metadata groups to be mapped and
evicted independently. A slot should correspond to a fixed-size or limited-scope
resource, such as one allocator slab. Slots are intended for metadata, not for
unbounded application data.

The persistence model is still global. All changed slots are persisted as part
of one atomic `MetaSpace` commit. Compaction is also global across all slots.

## Slot Address Encoding

Slot identity is encoded in the logical page number. The proposed split is:

```text
high 40 bits: slot id
low 24 bits: within-slot page id
```

With 16 KiB DPs, a 24-bit within-slot page id addresses roughly 256 GiB per
slot. If the implementation reserves ids or uses a smaller effective range, the
addressable space is still expected to be far larger than needed for
fixed-scope metadata slots.

The page-id encoding must be treated as part of the durable format once
persisted. Helpers should be used instead of open-coded bit manipulation so the
split can be audited and versioned.

## Slot Mapping Policies

The multi-slot runtime supports three mapping policies:

- `eager`: all slots are memory-mapped on startup. This is the default.
- `lazy`: slots are mapped on demand when data from the slot is accessed.
- `mixed`: selected slot groups are mapped lazily while others are mapped
  eagerly.

The expected mixed-mode use case is to keep critical or frequently used
metadata eager while mapping no-cache or low-priority metadata lazily.

Lazy loading uses range queries over the associated sparse pair. Because slot id
is encoded into the high bits of the page number, a slot load can retrieve all
logical page mappings in the slot with a range scan:

```text
[slot_id << 24, (slot_id + 1) << 24)
```

Each returned mapping gives the full DP location and diff sequence needed to
materialize the page into the slot-local mapping.

## Atomic Commit Across Slots

Slot independence is a memory-management property, not a transactional
property. The persistence algorithm must commit all slot changes atomically.

Commit requirements:

- Dirty pages from all mapped slots participate in the same head-state commit.
- Lazy slots with no loaded or dirty pages do not need to be materialized merely
  because another slot is committed.
- Sparse-pair updates for all changed slots are published as one generation.
- Recovery must not observe a commit where only some slots advanced to the new
  generation.
- Compaction rewrites the head state consistently across all slots.

## BDevStorage Integration

The multi-slot `MetaSpace` store is integrated with `BDevStorage` as a separate
dedicated page-IO channel.

Its primary responsibility is hosting the main sparse pair that maps
application-level data pages to their physical storage locations and diff
chains. The `MetaSpace` itself also needs metadata describing its own page
locations. That self-metadata sparse pair is maintained by the root-level
`DRAM_Prefix`, avoiding recursive dependency on the multi-slot `MetaSpace`
being opened.

The storage layering is:

```text
BDevStorage
  application data page channel
  MetaSpace page-IO channel
    main sparse pair for application data pages
  root-level DRAM_Prefix
    sparse pair for MetaSpace's own metadata pages
```

## Open Questions

The implementation should resolve these details before coding:

- The exact durable format for sparse-pair values: full DP location, diff
  sequence encoding, generation id, and checksums.
- The generation publication protocol used to choose head versus historical
  state during recovery.
- The diff-stream size cap and whether it is configured by byte size, block
  count, replay cost estimate, or a combination.
- The operational behavior when compaction is suspended and the diff cap is
  reached.
- The public or internal API shape for suspending and resuming compaction.
- The slot policy configuration format and whether policies are global,
  per-slot, or per slot group.

## Test Plan

Follow TDD when implementing this design.

Required storage-level tests:

- A metadata page can be written as a full DP and reopened through the sparse
  pair mapping.
- Multiple updates to a metadata page are persisted as diff appends and replay
  in order.
- Recovery uses the previous historical generation if a crash is simulated
  before the new head generation is fully published.
- Old full DP locations are reused only after the previous generation is no
  longer needed.
- Compaction rewrites diff-backed head pages as full DPs and clears the diff
  stream.
- Suspended compaction postpones administrative rewrite work without breaking
  ordinary diff-backed commits below the cap.

Required multi-slot tests:

- Eager mode maps all slots on startup.
- Lazy mode maps a slot only after accessing a page in that slot.
- Mixed mode eagerly maps configured slots and lazily maps configured lazy
  slots.
- Slot load uses sparse-pair range lookup and reconstructs all pages in the
  slot.
- A commit containing dirty pages from multiple slots is recovered atomically.
- Compaction covers all slots and leaves no stale diff dependencies for the new
  head generation.

