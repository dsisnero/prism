CRYSTAL ?= crystal
AMEBA   ?= ameba
SHARDS  ?= shards

.PHONY: all install update format format-check lint test bench docs clean sidecars

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
	$(CRYSTAL) tool format src spec bench

format-check: sidecars
	$(CRYSTAL) tool format --check src spec bench

lint: sidecars
	$(AMEBA) src spec bench

test: sidecars
	$(CRYSTAL) spec

bench: sidecars
	$(CRYSTAL) run --release bench/main.cr

docs: sidecars
	$(CRYSTAL) docs -o api

clean: sidecars
	@mkdir -p temp
	@find temp -mindepth 1 -delete
	@rm -rf .crystal-cache api
