# Resuming Work

This is the first thing to read in a new session. It takes a few minutes to get
back to a known-good state.

## 1. Read where we are

- [`plans/plan.md`](../plans/plan.md) — start at the **Where we are** block. It
  names the current focus, the ordered "Next up" list, per-phase status, and
  the last commit and gate results. Do not re-derive state from git history.
- The current phase's checklist lower in the same file — it is the working
  queue for this session.

## 2. Confirm the working tree

```bash
git log --oneline -10
git status
```

The last commit should be the previous feature, committed atomically. If the
tree is dirty, finish or stash before starting.

## 3. Confirm a clean baseline

```bash
make lint
make test
```

Both must pass before changing anything. The first `make test` compiles
`arrow-cr` and `parquet-cr` and takes roughly 20–60 s on a warm cache; expect
longer from cold.

## 4. Pick the next feature

Take the first unchecked item in **Next up** (or the next `[ ]` in the current
phase). For each feature:

1. Confirm what the dependency actually provides before designing around it —
   read the relevant file under `lib/arrow-cr/src/` or `lib/parquet-cr/src/`
   (both are symlinks to sibling checkouts). Do not guess signatures.
2. Write the spec first (red → green).
3. Satisfy the **Definition of done** in `CLAUDE.md` (API, specs, gates, plan
   status + **Where we are** refresh, divergence notes).
4. Commit atomically using the `feat: ...` message format.

## 5. Check dependency state

```bash
ls -l lib/                 # arrow-cr and parquet-cr should be symlinks
shards check               # or: shards install
```

If `lib/arrow-cr` is missing, run `make install`. Both dependencies are local
path shards; see [development.md](development.md#dependencies).

## If the plan is split

When `plans/plan.md` grows too large it is split into `plans/<phase>.md` files.
In that case every split file is linked from `plans/plan.md`, which marks each
file complete/in-progress and names the file being worked on plus the next one
to start.

## Environment notes

This checkout lives on an exFAT volume, which writes AppleDouble `._*` sidecars.
A `._foo_spec.cr` sidecar matches Crystal's `*_spec.cr` glob and breaks test
discovery, so the `Makefile` strips them before `format`/`lint`/`test`. If git
prints `non-monotonic index .../._pack-*.idx`, delete them with:

```bash
find .git -name '._*' -delete
```
