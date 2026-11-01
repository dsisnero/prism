# PR Workflow

## Branching

Branch from `main`, one feature per branch, named for the phase and scope:

```bash
git switch -c phase-3/sort-by-multi-key
```

Phases are defined in [`plans/plan.md`](../plans/plan.md). If a feature spans two
phases, split the PR.

## Commits

One atomic commit per completed feature is the norm for this repository; a PR
may contain several. Format:

```text
feat: <what now works>

- bullet: what changed, which files/phase
- bullet: semantics decisions or divergences
- gates: make lint + make test green
```

Prefixes: `feat:`, `fix:`, `docs:`, `chore:`, `perf:`, `test:`.

Never commit:

- anything under `lib/` (symlinked dependencies),
- `.shard.lock` (libraries do not lock),
- generated fixtures in `temp/`,
- macOS `._*` sidecar files.

## Before opening a PR

```bash
make format-check
make lint
make test
```

All three must pass locally. Verify the tree is clean of junk:

```bash
git status --short
find . -name '._*' -not -path './.git/*'
```

## PR description

Answer these explicitly:

1. **Phase and scope** — which plan item this completes.
2. **API** — new public methods, with one usage snippet each.
3. **Semantics** — null handling, dtype rules, chunking behaviour. If anything
   diverges from Arrow/Parquet behaviour, say so and link the plan section.
4. **Tests** — which spec files were added and what the key cases are.
5. **Dependencies** — did you need a change in `arrow-cr` or `parquet-cr`? If
   yes, link that PR. This repo never patches them locally.
6. **Gates** — paste the `make lint` / `make test` result lines.

## Review focus

Reviewers should check, in order:

- **Correctness under nulls and empty input** — the most common source of silent
  bugs in dataframe code.
- **Schema fidelity** — a verb must not change dtypes or nullability by
  accident.
- **Copy discipline** — did the change concatenate batches it did not need to?
- **Error messages** — do they name the column?
- **Plan hygiene** — is the plan's checklist and **Where we are** block updated?

## Merging

Squash-merge with the commit message above. After merge, the author updates
`plans/plan.md`'s **Where we are** block on `main` if the PR left it stale (it
should not — it is part of the definition of done).

## Reporting dependency bugs

Open the issue against the dependency repository, not here, and add a line to
`plans/plan.md` §15 with the reproduction and the workaround. If the workaround
is temporary, say what unblocks its removal.
