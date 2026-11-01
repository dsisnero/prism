# Testing

## Running specs

```bash
make test                              # full suite
crystal spec spec/prism/expr_spec.cr   # one file
crystal spec spec/prism/expr_spec.cr:42  # one example
```

The full suite compiles `arrow-cr` and `parquet-cr` every run. That dominates
wall-clock time (roughly 20–60 s warm) regardless of how many specs you run, so
targeting a single spec file buys little on a cold cache but a lot on a warm
one. Batch your edits rather than running after each line.

## Layout

```text
spec/spec_helper.cr        # requires ../src/prism
spec/prism/dataframe_spec.cr
spec/prism/series_spec.cr
spec/prism/values_spec.cr
spec/prism/expr_spec.cr
spec/prism/expr_eval_spec.cr
spec/prism/verbs_spec.cr
spec/prism/group_by_spec.cr
spec/prism/join_spec.cr
spec/prism/io/parquet_spec.cr
spec/prism/io/csv_spec.cr
spec/prism/lazy_spec.cr
spec/prism/optimizer_spec.cr
spec/prism/typed_schema_spec.cr
spec/prism/display_spec.cr
```

Specs mirror `src/prism/`. A new source file with non-trivial logic gets a
matching spec file, even if it only holds shared helpers.

## What every feature must test

1. **Values** — the happy path, asserted on content, not just shape.
2. **Schema** — resulting `Arrow::Schema` including field names, dtypes, and
   nullability. A verb that silently widens `Int64` to `Float64` is a bug.
3. **Nulls** — null in, null out; null predicates; all-null columns;
   null group keys.
4. **Empty input** — zero rows in gives zero rows out with the *correct schema*.
5. **Errors** — misspelt column raises `Prism::ColumnNotFound` naming the
   column; incompatible dtypes raise `Prism::DtypeError`.
6. **Chunking** — element-wise verbs preserve the chunk count; ordering verbs
   collapse to one batch (see [architecture.md](architecture.md#chunking)).

## Expressions

`Expr#==` returns an `Expr`, not a `Bool`. This makes normal matchers useless:

```crystal
# WRONG — passes vacuously; any non-nil Expr is truthy
(col("a") == 1).should eq(col("a") == 1)

# RIGHT — structural comparison
(col("a") == 1).same_as?(col("a") == 1).should be_true
(col("a") == 1).key.should eq("bin:eq(col:a,lit:1)")
```

`!expr` is not overloadable in Crystal (it is a pseudo-method), so negation is
`.not`; a spec that expects `!expr` to build an expression will not compile.
Boolean composition must be parenthesised, because `&` binds tighter than `==`.

Also assert that `dtype(schema)` and `eval(...).data_type` agree, via a shared
helper, so the two can never drift:

```crystal
def assert_dtype_agrees(expr : Prism::Expr, df : Prism::DataFrame)
  inferred = expr.dtype(df.schema)
  actual = expr.eval(Prism::EvalContext.new(df.to_arrow)).data_type
  actual.should eq(inferred)
end
```

Operator-precedence cases belong in a spec too: `a & b == c` must be written
with parentheses, and a spec should pin that the parenthesised form produces the
intended tree.

## IO specs

Do not commit fixture binaries. Generate them:

```crystal
io = IO::Memory.new
df.write_parquet(io)
io.rewind
back = Prism.read_parquet(io)
```

For a real file, write to `temp/` (gitignored) inside the spec and read it back.
Parquet round-trips through `IO::Memory` cover the writer/reader contract; the
one file-based spec exists to prove path handling, not format correctness.

## Optimizer specs

Assert plan shape, not only results, so a quietly disabled pass fails:

```crystal
plan = Prism.scan_parquet("temp/x.parquet")
  .filter(col("price") > 10)
  .select("price", "region")
  .explain

plan.should contain("projection: [price, region]")
plan.should contain("predicate: col:price > lit:10")
```

Plus an equivalence spec: the optimized plan and the naive eager pipeline must
produce identical frames for the same input.

## Compile-time specs

The typed-schema phase asserts that a misspelt field **fails** to compile:

```crystal
# writes temp/compile_fail.cr, then:
output = `crystal build --no-codegen temp/compile_fail.cr 2>&1`
$?.success?.should be_false
output.should contain("salry")
```

Keep these few and slow-path; they are the only way to test a compile-time
guarantee.

## Determinism

No randomness without a seed. Group order, sort order with ties, and null
placement must be asserted exactly — `group_by` sorts by keys, and that is part
of the contract.

## Markdown

Docs are linted with `rumdl` when installed (`.rumdl.toml` excludes `spec/`,
`temp/`, `lib/`, and `CHANGELOG.md`). Keep lines unwrapped but readable, one
sentence per line is not required, and always link with relative paths so the
docs work on GitHub and locally.
