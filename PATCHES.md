# Patches

This fork's `main` is upstream SlateDB `main` at `8c1c6c33` (2026-10-05, "move ownership of next_wal_id to manifest writer (#2142)") plus the patches below, in this order.
The previous base was upstream 0.17.0 (`c1e36fc`); that stack is still on `8510667e` and on the per-patch branches.
vlpds and vlRelay pin `main`'s head commit by rev.
This repo is exported from a monorepo, where the fork lives with its full history. MONO.md says how that works, how it's published and how to rebase it on upstream.
Each patch also has its own branch, so that it can be reported upstream later. The branches of patches 1 to 6 still hold the 0.17.0-based commits. Patches 7 to 9 have branches on the current base. Patch 9's branch, `cache-usage`, is `main`'s tip as of the move into the monorepo.
Add a row here with every new patch.

| # | Commit | Branch | What and why | Upstream |
|---|--------|--------|--------------|----------|
| 1 | dropped (was `f246143`, `308ffa5`) | `vlpds-0.17-union-l0-view-ids` | `Manifest::cloned_from_union` gave repeated L0 view ids fresh ids. A union clone of two sources that shared an L0 SST repeated its view id, and the first compaction of the merged shard dropped the keys of one half. | superseded by upstream #2132 |
| 2 | `35ec76d` | `vlpds-0.17-batch-next` | A synchronous `next` fast path for resident rows, and `DbIterator::next_batch`. A scan paid an awaited, boxed future per iterator layer per row (about 12.5k to 3.4k instructions per row). | pending report |
| 3 | `cd1a498` | `vlpds-0.17-submit-dest-guard` | `validate_compaction` fails a submitted spec that collides with a claimed job. An admin-submitted compaction skipped the destination check, and the executor's `assert!` panicked. | pending report |
| 4 | `d82bf1a` | `vlpds-0.17-filtercache` | `CompactionWorkerBuilder::with_db_cache` seeds a standalone worker's output SSTs into the DB cache. Without it, the first reads of a fresh SST always missed its index and filters. | pending report |
| 5 | dropped (was `c7b29a0`) | `fix/l0-view-merge-dup-sst` | `LsmTreeState::merge_writer_and_compactor` cut L0 at the compacted view id, not the SST id. A union clone has one SST behind several views, and the cut dropped live views, so the next flush failed with `InvalidClockTick`. | superseded by upstream #2134 |
| 6 | `3984093` | `vlpds-0.17-cache-first-loader` | The table store peeks the cache (new `DbCache::peek_*`) before it builds a `CacheLoader`. Every cache hit built a loader that it did not use, about 10 allocations per section read. | pending report |
| 7 | `9136da6` | `fix-send-after-close` | `SafeSender::closed_send_error` returns `BackgroundTaskCancelled` when the channel closed with no close result recorded, instead of panicking. A runtime that shuts down drops the DB's background tasks without recording one, so a later write from another runtime panicked ("Failed to send message to unbounded channel"). | pending report |
| 8 | `9bbeb84` | `fix-fetch-clamp` | `SplitCache::fetch_block` and `fetch_index` clamp the loaded entry to its own allocation, as `SplitCache::insert` does. The dedup fetch path (single-block reads, index loads) stored the loader's entry as-is: uncompressed, a block or index is a `Bytes` slice of the object-store response, which can be a slice of hyper's read buffer (~250 KiB), so each ~4 KiB cached block pinned a whole buffer the cache never weighed (vlRelay's uncompressed PLC seeds grew past 1.8 GB live against a 320 MB cache). | pending report |
| 9 | `87db9e3` | `cache-usage` | `DbCache::weighted_size` (bytes held, as the weigher counts them) and `split_weighted_size` (`(block, meta)` for `SplitCache`), both defaulting to "unknown" (0 / `None`) so other caches still compile. `FoyerCache` and `FoyerHybridCache` (memory tier) report foyer's `usage()`, and their `entry_count` now returns foyer's `entries()` instead of a hard 0. vlRelay exports them to check its shared cache against its budget. | pending report |

Patches 2, 4 and 6 were adapted to the new base: their tests call `Db::snapshot` without `.await` (#2138) and pass a `ReadTrace` to `read_blocks_using_index` and `MergeOperatorIterator::new` (read-path tracing, #2096 and #2104). Patch 4 also keeps upstream's new `sst_block_alignment` field next to its own in `CompactionWorkerBuilder`.

## Superseded patches

Patch 1, superseded by upstream #2132.
#2132 fixes the same bug twice over. `SsTableView::try_with_visible_range` gives a view a new id (same timestamp) whenever projection changes its visible range, so the two halves of a split no longer share their parent's view ids.
`Manifest::assign_union_l0_view_ids` then gives any id still repeated within one tree's L0 a fresh id when the union is built, like our patch did.
On top of that, `Compaction::get_l0_sst_views` now fails with `InvalidCompaction` on a missing, repeated or ambiguous L0 source, and `validate_compaction` calls it, so a manifest with duplicate ids can no longer lose a view in compaction.
Upstream's `test_union` cases assert unique union view ids, and `clone::tests::should_preserve_union_data_after_compaction_and_reopen` is an end-to-end version of our bug: split a parent into four projections, write to each, union them, compact, and check every key. Both pass on the new base without our patch.

Patch 5, superseded by upstream #2134.
#2134 makes the same change to the same `take_while`: with a view watermark, the cut matches the view id only, and the SST id is a fallback for a manifest without one.
Our regression test (`test_lsm_tree_merge_cuts_at_view_id_when_an_sst_backs_several_views`) passes on the new base without our patch, as does upstream's `test_watermark_matches_view_not_shared_sst`.

## Format and compatibility (c1e36fc to 8c1c6c33)

No persisted format changes the bytes that 0.17.0 writes, as long as SST block alignment stays off. Both builds open what the other wrote.

- Manifest and compactor state: `schemas/manifest.fbs`, `schemas/compactor.fbs` and `schemas/root.fbs` are unchanged, and so are the format version constants.
- #2142 (next_wal_id ownership) is in-memory only. The writer's in-memory `next_wal_sst_id` no longer advances on every WAL flush. The manifest writer samples the WAL status when it writes a manifest, so the persisted `next_wal_sst_id` field keeps its meaning (the next WAL SST id). Old and new builds read it the same way.
- SST and WAL SSTs: #2102 adds `BlockMeta.encoded_len` (`schemas/sst.fbs`) for optional block alignment (`with_sst_block_alignment`, off by default; the builder rejects it with compression). An unpadded block writes 0 there (`encoded_len_for_index`), which flatbuffers leaves out, so the index bytes are the same as 0.17.0's. A reader treats 0 as "the distance to the next block", which is what 0.17.0 does. Do not turn alignment on while any 0.17.0 build can read the data: an older reader would count the padding as block bytes and fail the checksum.
- #2132 and #2143 change values, not formats. Projections and unions write new view ids, and clones and unions start `recent_snapshot_min_seq` at `last_l0_seq` instead of inheriting the source's. An old build reads both fine.
- Behaviour on old data: a new compactor refuses (`InvalidCompaction`) a compaction whose L0 sources include an id repeated in L0. Such a manifest could only come from a union built by a SlateDB without our patch 1 or #2132. Projection and union creation now assert (panic) that no live L0 view carries its tree's `last_compacted_l0_sst_view_id`. Compaction removes the watermark view from L0, so only a manifest that the duplicate-id bug of patch 1 damaged could trip it.
