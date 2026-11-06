# `arrow-cr`: the CSV reader costs ~1.4x a stdlib lexer plus the same builders

## Status: fixed upstream (2026-11-06)

Fixed in `arrow-cr` `fab3c36` ("perf: build CSV strings from bytes, scan plain
runs unchecked"), which finishes the work `59381cd` started. Consumers see it
after `shards update arrow-cr` (the pin here was `ca106f2`).

Measured on the report's own scenario (1e6 rows x 4 columns, all-Utf8 schema,
`--release`, best of five, same-machine alternating runs):

```text
                              before (ca106f2)      after (fab3c36)     stdlib (same runs)
arrow-cr reader (all-Utf8)    367 - 511 ms          201 - 227 ms        201 - 258 ms
arrow-cr reader (Int32 col)   424 ms                358 ms              -
```

So the reader is now **level with** Crystal's stdlib `CSV.parse` plus the same
four `StringArray` builders instead of ~1.9x, i.e. well inside the 1.25x
acceptance bar, and a typed read stays at 1.44x the all-Utf8 read. The
`IO::Memory`/double-`dup`/validation-`String` costs the report named were real
and are all gone: unquoted fields are slices of the fed buffer, the byte loop
scans plain runs without a per-byte call, `flush` keeps rows as bytes and feeds
the Utf8/Utf8View builders directly, and UTF-8 validation is a non-allocating
byte validator with the same rules as `String#valid_encoding?`.

What is left (not worth a report): typed columns still materialize one `String`
per cell for the parsers, and the parser still builds one `Array(Bytes)` per
row. Neither is on the all-Utf8 path any more.

### Progress (2026-11-06, `arrow-cr` `59381cd`)

### Progress (2026-11-06, `arrow-cr` `59381cd`)

The per-cell allocation analysis below was correct and the first part is fixed
("perf: stop copying every CSV field twice"):

- `parse_record` no longer allocates an `IO::Memory` per field: unquoted fields
  are slices of the fed buffer, and only a field that touched a quoted section
  is materialized (through one scratch buffer reused across fields).
- The delimiter/quote/terminator tests are inlined in the byte loop instead of
  a `terminator_at` call per byte.
- `flush` builds each field as exactly one `String` and validates it there;
  the separate `validate_utf8` pass (one throw-away `String` per cell) is gone.

Measured on the same machine with a 1e6-row x 4-column all-Utf8 file (28 MiB,
`--release`, best of five): reader **730-900 ms before, 546-586 ms after**
(~1.35-1.55x faster); the stdlib `CSV.parse` + four `StringArray` builders side
measured 277-310 ms in the same runs. That is ~1.9-2.0x, so the 1.25x bar is
still open. What remains, in rough order of expected payoff:

1. The reader still builds the rows twice: `Array(Array(Bytes))` in the record
   decoder, then `Array(Array(String))` in `flush` before the column builders.
   Building columns straight from the byte records (with a byte-level UTF-8
   validator for the unprojected fields) would remove one 1M-array allocation
   pass.
2. The byte loop is still a bounds-checked Crystal loop with a branch per byte;
   the stdlib lexer scans for the next delimiter instead.
3. `line_count` re-scans every consumed record to count line breaks, although
   `parse_record` already sees each byte; counting quoted newlines there would
   remove one pass over the input.
4. `feed` still copies the whole first buffer (`input.dup`); `BufReader` owns
   its bytes and could hand ownership over.

Prism's acceptance oracle (CSV specs plus
`spec/prism/io/roundtrip_spec.cr`) is unchanged and green in `arrow-cr`'s own
55 CSV fixtures; the current pin here is still `ca106f2`, so a
`shards update arrow-cr` is needed before re-measuring Prism itself.

### Original report (2026-11-05)

## Failure

Reading a CSV is roughly 40% slower than it needs to be. On the same bytes, the
same four `StringArray`s come out **1.4x slower** through the reader than through
Crystal's stdlib `CSV` lexer plus the identical builders — so this is the
reader's per-cell path, not typed parsing: no dtypes are involved in the
comparison at all (`read_csv(path, schema: all_utf8)`).

Reproduce with the checked-in scenario (`make bench`, 1e6 rows x 4 columns,
31.9 MiB, best of five, Crystal 1.22.2, `--release`, Apple M4, machine load
~4.5):

```text
| write_csv                  | 229.8 | 231.8 | 1e6 rows, 4 columns       |
| read_csv (all columns)     | 445.1 | 452.1 | schema supplied           |
| read_csv (inferred)        | 797.9 | 813.0 | infer_rows=100, two passes |
```

The apples-to-apples comparison the report rests on, same file, all-Utf8 schema
so the reader does no type parsing (three runs, best of five each):

```text
arrow-cr reader, all-Utf8 schema                     511.9 / 519.7 / 524.3 ms
stdlib CSV.parse + the same four StringArray builds  356.3 / 358.8 / 376.9 ms
ratio                                                       1.39 - 1.46x
```

The stdlib side is a plain `CSV.parse(File.read(path))` (which materialises every
cell as a `String`) followed by four `Arrow::StringArray.from_nullable` builders
— i.e. *more* allocation than a streaming reader, and still faster.

## Where the time goes

`lib/arrow-cr/src/arrow/csv/reader.cr`, per field:

- `parse_record` (line 236) allocates an `IO::Memory` per field and copies the
  cell into it, then `fields << field.to_slice.dup` (lines 243, 255, 264, 292,
  299) copies it again — two allocations and two copies per cell before the
  array builders see anything. For 4M cells that is 4M `IO::Memory` objects and
  4M intermediate slices.
- `validate_utf8` (line 462) calls `String.new(bytes)` per cell **only to check
  `valid_encoding?`** and throws the `String` away — a third allocation per cell
  that is pure overhead in the common all-ASCII/UTF-8 case.
- `flush` (line 430) then maps every cell to `String.new(bytes)`, the fourth
  allocation, before `build_batch` parses what it needs.

Crystal's stdlib lexer scans into slices of the source buffer and allocates one
`String` per cell; nothing else. `arrow-rs` is not a fair reference here either —
its `arrow-csv` reader delegates lexing to the `csv` crate, so this port wrote
its own and inherited the cost.

## Expected

Within ~1.25x of the stdlib lexer plus the same builders for the all-Utf8 case,
which is the acceptance bar for "the reader is not the bottleneck". The typed
read should stay within ~2x of the all-Utf8 read (parsing is real work).

## Suggested fix

1. Parse into slices of the fed buffer instead of an `IO::Memory` per field:
   `parse_record` already scans `@buffer` byte by byte, so it can record
   `(start, len)` per field and slice once, with the existing `dup` at
   end-of-batch if the buffer must not be retained. (Alignment with upstream's
   `csv` crate is not lost — the crate also yields borrowed slices.)
2. Validate UTF-8 on the bytes directly (`String.new(bytes).valid_encoding?`
   only when the buffer has a non-ASCII byte, or with a byte-wise validator),
   instead of materialising a throwaway `String` per cell.
3. Reuse the `fields` array and its `Bytes` across records (`clear` + refill)
   so a batch does not allocate 4M small objects.

Correctness must not move: the CSV specs in `spec/arrow/csv/` (delimiters,
quotes, escaped quotes, multi-line fields, comments, truncated rows, `header`
and `header_validation`) and the round-trip oracle in Prism
(`spec/prism/io/roundtrip_spec.cr`) are the acceptance tests for the rewrite.

## Prism side (why this is not fixed here)

A stdlib-lexed fast path was measured and declined
([§17 of the plan](../plans/plan.md)): the typed half is `arrow-cast`'s parsers
(dates, times, timestamps with units and timezones, durations, intervals,
decimals), which are exactly what makes `read_csv(schema:)` round-trip exactly,
and `CSV.parse` materialises every cell where the reader streams
`batch_size`-row chunks. A second reader inside Prism would be one more thing to
keep aligned with Arrow's semantics; the fix belongs in the `arrow-cr` lexer.

`batch_size` was ruled out first as a cheaper explanation: 1k/4k/16k/64k/256k
all land within noise (756-895 ms on the 17 MiB probe file), so a Prism-side
default change does not recover it.

## History

Measured while closing Prism's IO round-trip oracle (Phase 32) and the CSV
schema-by-name fix (Phase 33). The CSV writer is *not* part of this report:
`write_csv` measured 229.8 ms here against 251-500 ms for a stdlib builder fed
pre-formatted cells, i.e. a wash.
