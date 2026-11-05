# `arrow-cr`: the CSV reader costs ~1.4x a stdlib lexer plus the same builders

## Status: open (2026-11-05)

Prism does not work around this: `read_csv` stays on `arrow-cr`'s reader, whose
typing and inference are the contract ([Phase 32 of the
plan](../plans/plan.md)). The report exists so the gap is measured rather than
remembered, and so the fix lands upstream where every consumer gets it.

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
