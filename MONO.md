# This fork in the monorepo

github.com/jazware/slatedb is exported from Jaz's monorepo, where the fork lives at `packages/slatedb`.
Changes are made there. The public repo's `main` only fast-forwards to what the export makes.
PATCHES.md stays the patch ledger. Add a row there with every new patch, as before.

## How it's stored

The monorepo holds every commit of the fork, from upstream's first commit on, rewritten so that its tree sits at `packages/slatedb`.
Only the `tree` and `parent` lines of each commit change. Authors, dates, messages and GitHub's signatures are kept byte for byte.
A merge commit then joins that history to the monorepo's.
The import was `cache-usage` at `87db9e3b`, which contains `main` (`11b27b89`) and `fix-fetch-clamp` (`9bbeb849`).

`scripts/slatedb-public/slatedb_public.py` in the monorepo does both directions:

- `import` rewrites a fork commit and its ancestors under `packages/slatedb`. It's a pure function, so importing a later commit of the same history reproduces every commit an earlier import made.
- `export` puts each imported commit's tree back at the root, which gives back the original commit under its original SHA. Each monorepo commit that changes `packages/slatedb` becomes one new commit on top, with its message, author and dates, a `Mono-Commit:` trailer and a few private names scrubbed. Merges that change nothing here are skipped, as `git subtree split` does. An audit fails the export if a new commit's message or added files hold a private host, address or secret.

So `git log -- packages/slatedb` in the monorepo shows the fork's whole history, and the export reproduces every published commit and only appends.

## Publishing

The `sync-slatedb` yeet job runs `scripts/slatedb-public/sync.sh`:

1. `export`: the export, twice (the two HEADs must match), and a check that it still contains `87db9e3b`.
2. `cargo`: `cargo check --workspace --all-targets --all-features`, `cargo nextest run --workspace --all-features --profile ci-cross` and the doc tests, as this repo's CI runs them. The DST and bindings jobs are left to upstream's CI. A run whose export is published already skips them.
3. `push`: a fast-forward of `main`, after checking that the public `main` is an ancestor of the export. It never force pushes. The push goes to the `slatedb` repo on the monorepo's forge (https://delta.jazco.dev/slatedb), which mirrors `main` here.

By hand, from a git checkout of the monorepo: `DRY_RUN=1 scripts/slatedb-public/sync.sh all` does everything but the push.
The per-patch branches aren't exported. Push them from a scratch clone of the export when a patch is reported upstream.

The `slatedb-upstream-drift` job runs daily. It reports how many commits the export is behind and ahead of upstream's `main`, and for each PATCHES.md row whether upstream has applied that patch as is (`git cherry`) or merged a PR the row names. It changes nothing and fails only on errors.

## Rebasing on upstream

The public `main` can't be rewritten, so a rebase reaches it as a merge whose second parent is the rebased stack.

1. Export the monorepo's current fork and rebase it in a scratch clone:
   ```sh
   python3 scripts/slatedb-public/slatedb_public.py export --src <git checkout of the monorepo> --ref main --out /tmp/slatedb
   cd /tmp/slatedb
   git fetch https://github.com/slatedb/slatedb main
   git rebase FETCH_HEAD          # resolve, adapt the patches, update PATCHES.md's base and rows
   cargo nextest run --workspace --all-features --profile ci-cross
   ```
2. Import the result into your delta repo (in the monorepo):
   ```sh
   python3 scripts/slatedb-public/slatedb_public.py import --src /tmp/slatedb --rev HEAD --out /tmp/slatedb-import.git
   jj delta import-git --no-push /tmp/slatedb-import.git
   jj new main@origin slatedb-import
   jj restore --from slatedb-import packages/slatedb
   jj bookmark delete slatedb-import
   jj describe -m "slatedb: rebase onto upstream <sha>"
   ```
   `jj restore` takes the rebased tree whole, whatever conflicts the merge had.
3. Get it onto `next`. `jj land` takes linear stacks only, and the forge's `next` refuses merge commits, so an admin moves `next` to the merge by hand while that policy is lifted, the way the first import went in. Land it while `next` equals `main`, and cut right after, since an eject below a merge can't rebase past it.
4. The export turns that merge into a commit with the old public `main` and the rebased tip as parents and the rebased tree, so `main` fast-forwards.

Taking upstream as a merge instead (`git merge FETCH_HEAD` in step 1) goes through the same import. A rebase keeps one commit per patch on top of upstream, which is what the per-patch branches and upstream reports want.
