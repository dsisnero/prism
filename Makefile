CRYSTAL ?= crystal
AMEBA   ?= ameba
SHARDS  ?= shards

.PHONY: all install update format format-check lint test examples bench docs clean sidecars

all: format-check lint test

# macOS AppleDouble sidecars on exFAT volumes match Crystal's *_spec.cr glob
# and break test discovery, so strip them before every code-facing target.
sidecars:
	@find . -name '._*' -not -path './.git/*' -delete 2>/dev/null || true

install:
	$(SHARDS) install

update:
	$(SHARDS) update

format: sidecars
	$(CRYSTAL) tool format src spec bench examples

format-check: sidecars
	$(CRYSTAL) tool format --check src spec bench examples

lint: sidecars
	$(AMEBA) src spec bench examples

test: sidecars
	$(CRYSTAL) spec

# Builds and runs the examples; `spec/examples_spec.cr` does the same for CI.
examples: sidecars
	$(CRYSTAL) run --no-color examples/quickstart.cr
	$(CRYSTAL) run --no-color examples/parquet.cr

bench: sidecars
	$(CRYSTAL) run --release bench/main.cr

docs: sidecars
	$(CRYSTAL) docs -o api

clean: sidecars
	@mkdir -p temp
	@find temp -mindepth 1 -delete
	@rm -rf .crystal-cache api
