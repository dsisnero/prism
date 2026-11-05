# Coming from Polars

Prism borrows Polars' expression engine and lazy plan (§4.1 of the
[implementation plan](../plans/plan.md#41-design-influences)), so most of the
vocabulary carries over. This page maps the operations you already know onto
Prism, then lists the places where the semantics differ **on purpose** — each one
has a spec, and most are recorded with the measurement or upstream reference that
decided it.

Where Prism matches a Polars rule, the rule is stated here, so this page is
usable without leaving the repo; the [Polars reference
docs](https://docs.pola.rs/api/python/stable/reference/dataframe/index.html) and
its [DeepWiki](https://deepwiki.com/pola-rs/polars) are the authority for anything
that is only Polars'.

## At a glance

| Task | Polars | Prism |
| --- | --- | --- |
| Column reference | `pl.col("x")` | `col("x")` (after `include Prism::DSL`) |
| Literal | `pl.lit(1)` | `lit(1)` |
| Filter rows | `df.filter(pl.col("x") > 1)` | `df.filter(col("x") > 1)` |
| Project columns | `df.select("a", "b")` | `df.select("a", "b")` |
| Project an expression | `df.select((pl.col("a") * 2).alias("b"))` | `df.with_column("b", col("a") * 2)` — see [differences](#differences-that-bite) |
| Add/replace columns | `df.with_columns(...)` | `df.with_column(name, expr)` / `df.with_columns(name: expr)` |
| Drop columns | `df.drop("a")` | `df.drop("a")` |
| Rename | `df.rename({"a": "b"})` | `df.rename({"a" => "b"})`, or `df.rename("a", "b")` |
| Row index | `df.with_row_index()` | `df.with_row_index()` |
| Sort | `df.sort("a", descending=True, nulls_last=True)` | `df.sort_by("a", order: :desc, nulls: :last)` |
| Multi-key sort | `df.sort(["a", "b"], descending=[False, True])` | `df.sort_by(col("a").asc, col("b").desc)` |
| Top *k* | `df.top_k(10, by="price")` | `df.top_n(10, "price")` |
| Bottom *k* | `df.bottom_k(10, by="price")` | `df.top_n(10, "price", order: :asc)` |
| Group and aggregate | `df.group_by("k").agg(pl.col("v").sum())` | `df.group_by("k").aggregate(total: col("v").sum)` |
| Named aggregate | `... .agg(pl.col("v").sum().alias("total"))` | `df.group_by("k").sum("v")` keeps the input name |
| Distinct | `df.unique(subset=["k"], maintain_order=True, keep="first")` | `df.distinct("k")` — always first, always input order |
| Null count in a key | `pl.col("k").n_unique()` (null counts as one) | `series.n_unique` — same rule |
| Join | `df.join(other, on="k", how="left")` | `df.join(other, on: "k", how: :left)` |
| Concatenate rows | `pl.concat([a, b])` | `Prism.concat([a, b])` |
| Concatenate columns | `pl.concat([a, b], how="horizontal")` | `a.hstack(b)` |
| Lazy | `df.lazy()` … `.collect()` | `df.lazy` … `.collect` |
| Explain | `lf.explain()` | `lf.explain` / `lf.explain(optimized: false)` |
| Scan Parquet | `pl.scan_parquet(path)` | `Prism.scan_parquet(path)` |
| Read Parquet | `pl.read_parquet(path)` | `Prism.read_parquet(path)` |
| Write Parquet | `df.write_parquet(path)` | `df.write_parquet(path)` |
| Conditional | `pl.when(c).then(x).otherwise(y)` | `Prism.if_else(c, x, y)` |
| Chained conditional | `.when(c1).then(x1).when(c2).then(x2).otherwise(y)` | `Prism.case_when([{c1, x1}, {c2, x2}], y)` |
| Set membership | `pl.col("x").is_in(["a", "b"])` | `col("x").is_in(["a", "b"])` |
| Series membership | `s.is_in(other)` | `series.is_in(values)` |
| First non-null | `pl.coalesce(a, b)` | `a.coalesce(b)` |
| Fill nulls | `pl.col("x").fill_null(0)` | `col("x").fill_null(0)` (or the `df.fill_null(0)` verb) |
| Null mask | `pl.col("x").is_null()` | `col("x").is_null` |
| Cast | `pl.col("x").cast(pl.Int64)` | `col("x").cast(Arrow::DataType.int64)` (strict by default; `strict: false` nulls instead of raising) |
| Negate | `~expr` / `.not()` | `.not` (Crystal's `!` cannot be overloaded) |

## Differences that bite

**`select` is projection only.** Polars' `select` evaluates expressions; Prism's
takes column names and shows them as they are (RedAmber's meaning). Computed
columns go through `with_column`/`mutate`, which is also the spelling Polars uses
for `with_columns`. The collision is recorded in
[§7 of the plan](../plans/plan.md#7-deviations-from-the-proposed-sample-api).

**Null placement defaults differ.** Prism sorts nulls **last** unless you say
otherwise (`nulls: :first`). Polars' `nulls_last` defaults to `False`, which puts
nulls **first** for both directions. When porting, pass `nulls:` explicitly.

**Sorts are always stable.** Equal keys keep their input order in Prism, in both
the eager verb and the plan. Polars' sort is stable only with
`maintain_order=True`; otherwise the tie order is unspecified. Prism's
`distinct`/`unique` inherit the same promise: first occurrence, input order,
where Polars defaults to `keep="any"` with unspecified order.

**`top_n` returns sorted rows.** `top_k`/`bottom_k` select the same rows (the *k*
largest / smallest) but their output order is unspecified in Polars; Prism's is
`sort_by(...).head(k)` by construction, and `order: :desc` is the default because
"top" means largest. `top_n` also sorts only as far as the window — see
[benchmarks](benchmarks.md).

**Group order is deterministic by default.** `group_by` keeps discovery order
(`maintain_order: true`), where Polars' default is no guaranteed order. With
`maintain_order: false` the groups come out sorted by key, with nulls first
unless `null_keys: :last`; `null_keys: :drop` removes null-key rows. The
`null_keys:` option is Prism's own.

**`fill_null` only fills what the value fits — and skips the rest.** A
frame-wide `fill_null(0)` fills the integer and float columns and leaves strings,
booleans and lists untouched; a named replacement that cannot fill its column
raises `Prism::DtypeError` rather than being coerced into `"0"` or `[0]`.

This is a deliberate difference in the *frame-wide* form: Polars raises when a
column cannot hold the value (`fill_null(0)` on a frame with a string column is
an `InvalidOperationError` there), which makes the convenience form unusable on
the common mixed frame. Prism skips what it cannot fill — the wording its docs have
always used, "every column that can accept the value" — and the per-column form,
`Series#fill_null` and `Expr#fill_null` are strict, so a mistake still surfaces
whenever you name the column.

**Duplicate column names are rejected, which is stricter than Polars in one
place.** `rename` matches Polars (`DuplicateError` when the target name is taken),
and `group_by` cannot produce two columns of one name in either library. For
`select`, Polars tolerates duplicate output names — only assigning `df.columns`
raises — while Prism raises `Prism::DuplicateColumn`, because its `df[name]` is
single-valued: a second column of that name would be silently unreachable. A key
swap in `rename` and two aliases of one column are still fine, because the
*result* has unique names.

**`concat` is exact.** Names and dtypes must match field for field; there is no
`vertical_relaxed` that coerces to a supertype. The strictness is Prism's, not
Arrow's: `concat_batches` uses the caller's schema and only requires the arrays
to match, so a laxer nullability rule would work — it is refused because the
result would publish a non-null field that holds nulls. Nullability is the one
exception, and only for a frame with no rows, which cannot violate a nullable
contract, so concatenating results onto an empty accumulator works.
Horizontal stacking is `hstack`, which requires equal heights and no colliding
names — there is no `horizontal_extend` that pads with nulls.

**Joins keep Polars' vocabulary, including the sharp edges.** `how: :inner`,
`:left`, `:right`, `:full` (or its alias `:outer`), `:semi`, `:anti`; `on:` for
shared names, `left_on:`/`right_on:` otherwise; colliding non-key columns take
`suffix: "_right"`; **null keys never match** unless `nulls_equal: true`; key
columns are coalesced except for `full`, unless `coalesce:` says otherwise. That
is Polars' rule set, so a ported join usually needs only the symbol syntax
changed.

**Coercion follows the column.** Comparing or combining a column with a literal
casts the literal into the column's dtype when that is lossless, otherwise both
sides widen (`Int32` column with `1.5` runs in `Float64`). Arithmetic and
`if_else`/`case_when` share the rule. A comparison against a literal that cannot
fit — `col("i32") > 2^40` — folds to a constant rather than nulling every row.

**Boolean nulls are SQL/Polars Kleene logic.** `and`/`or` propagate only when the
result is genuinely unknown, so `true & null` is `false`. Use `&`/`|` with
parentheses or the precedence-safe `.and(...)`/`.or(...)` forms.

**`is_in` follows Polars on nulls.** Both default to `nulls_equal: false`, so a
null cell yields null rather than false; `is_in(values, nulls_equal: true)` makes
null a value, matching only when the set contains null. The set is a constant:
Prism encodes it once, so a 12k-element list does not become a 12k-term `|` chain.
It exists on both surfaces — `col("x").is_in(...)` as an expression and
`series.is_in(...)` as a mask — and they run the same helper, so a mask filter and
an expression filter agree row for row. Dictionary-encoded columns (Polars'
categoricals) compare through their **values**: Polars compares categoricals by
physical code but implicitly converts a string set into the categorical encoding,
and two dictionaries can map one code to different values, so decoding is the
encoding-independent answer — `Dictionary(Int32)` `["b","a","c"]` against
`Dictionary(Int8)` `["a","b"]` gives `[true, true, false]`. It pushes into the
Parquet reader like any other predicate, and the kernel is a hash probe — 1.2x a
hand-written `Set` on the same input, per [benchmarks](benchmarks.md).

**A cast never invents nulls.** Polars' cast is strict by default and so is
Prism's: `"abc"` to `Int64`, `300` to `Int8` and `NaN` to `Int64` raise
`Prism::DtypeError` naming the column, the count and a sample value instead of
becoming null. The rule is Polars' own — `cast_chunks` in
`crates/polars-core/src/utils/series.rs` compares the null count before and after
the cast (`check_nulls` is set for `CastOptions::Strict`) and calls
`handle_array_casting_failures`, which raises even when the column was already
nullable, reporting "conversion from `Utf8` to `Int64` failed for 1 out of 3
values: [\"abc\"]". Prism counts the same difference (`Coerce.cast_exact`), so a
strict cast behaves identically here, and `strict: false` is the same opt-out as
Polars' `CastOptions::NonStrict`: the value becomes null and the column is
declared nullable, because a non-strict cast is exactly the mode that invents
nulls. Prism's default is strict, so a lossy cast cannot pass silently. `Arrow.cast` is a *safe* cast, so
without this check a lossy cast silently rewrote data on a nullable column and
blew up inside `Arrow::RecordBatch` on a non-null one.

**A column's `nullable` flag is inferred from its expression.** Polars does not
carry a nullable flag at all — `Field` binds a name to a `DataType` and nulls
live in the `ChunkedArray` validity bitmap, so there is no Polars rule to port
here. The closest thing in the codebase is
`predicate_non_null_column_outputs` (`polars-plan/src/plans/aexpr/properties/`),
which reasons the same way about which expressions can materialize nulls. Prism's
version keeps that reasoning at the column level: `cast` keeps its operand's flag,
`fill_null` drops it (the nulls are gone), a comparison or arithmetic column is
null only where an operand is, and an aggregate stays nullable. The point is that
the eager verb and the plan answer it through one function (`Expr#nullable?`), so
`df.cast(...)` and `df.lazy.cast(...).collect` compose — the same `concat`
accepts both.

**Integer division by zero raises here, and nulls in Polars.** Polars'
`floordiv`/`mod` kernels return `null` when the divisor is zero; `arrow-cr`'s
checked kernels raise `Arrow::ArrowError: Divide by zero error`, and Prism
inherits that. It is the one place where the "an operand's nulls are the only
nulls" rule depends on a kernel choice: `Binary#nullable?` is only sound while
division raises instead of nulling (§17).

**CSV says what it cannot carry.** `infer_rows` is Polars' `infer_schema_length`,
including `0` meaning "read every column as string", and a window that holds no
value for a column falls back to string instead of a `Null` column — Polars
infers `String` there too, and a `Null` column would read every later value as
null. Writing refuses what has no faithful text form (binary, nested), which is
what Polars' CSV serializer does with `datatype {dtype} cannot be written to
CSV`; Prism's error names the column and points at Parquet or IPC. Parquet and
IPC carry every dtype exactly, so a round-trip that matters belongs there.

**A CSV schema is matched by name.** Polars matches a supplied schema to the
file's header by name, keeps the file's column order, and raises `SchemaMismatch`
for columns on one side only; Prism does the same (`Prism::SchemaError` naming
the missing and unexpected columns), and rejects two fields of one name with
`DuplicateColumn`. Two Polars knobs have no Prism spelling yet — `extra_columns`'
`ignore` and `missing_columns`' `insert` (§17) — because dropping or inventing a
column silently is the kind of convenience that hides a mismatch.

**`n_unique` counts null.** One null value counts as one distinct value, matching
Polars' `ChunkUnique` (which adds one when `null_count > 0`) and
`len(unique())`, and differing from SQL's `COUNT(DISTINCT x)`.

**A conditional's null condition takes the else branch.** Arrow's `zip` reads a
null mask as false and Polars' `when/then/otherwise` does the same, so
`case_when` means "first branch that matches" with no null special case.

## Deliberately out of scope

Window functions (`over`, cumulative ops, `rank`), `pivot`/`melt`/`unpivot`, SQL
and string-expression parsing, index/row labels, multi-threaded execution,
async/object-store IO, and the SQL-flavoured `select` above. The
[README](../README.md#status) lists these; `rank` and shifts are absent because
they are window functions, not because they were forgotten.

## Reading the numbers

Prism's benchmarks are recorded per scenario with the decision they settled
([benchmarks](benchmarks.md)), and the sort strategy decisions are spelled out in
§14 of the [plan](../plans/plan.md#14-zero-copy-policy-and-performance). Two
properties of the engine are worth knowing when comparing to Polars: Prism is
single-threaded, and its multi-key sort encodes keys into `arrow-row` bytes only
when two or more keys are variable-length — Polars does that for every multi-key
sort.
