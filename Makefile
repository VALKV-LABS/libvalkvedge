# Everything runs in Docker. No local Rust toolchain is needed.
IMAGE  ?= rust:1-bookworm
RUN     = docker run --rm -v "$(CURDIR)":/src -w /src \
          -v vke-cargo:/usr/local/cargo/registry -v vke-target:/target \
          -e CARGO_TARGET_DIR=/target -e CARGO_TERM_COLOR=always $(IMAGE)

.PHONY: help up unit integ lint fmt down cli-image

help: ## List the commands
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-10s %s\n", $$1, $$2}'

up: ## Start the S3 emulator used by the integration tests
	docker compose up -d --wait

unit: ## Unit and in-process tests (no external services)
	$(RUN) cargo test --workspace --locked

integ: ## Integration tests against the S3 emulator
	docker compose up -d --wait
	docker compose run --rm tests

lint: ## Format check and clippy, warnings as errors
	$(RUN) sh -c "rustup component add rustfmt clippy >/dev/null 2>&1; cargo fmt --all --check && cargo clippy --workspace --all-targets --locked -- -D warnings"

fmt: ## Format the code
	$(RUN) sh -c "rustup component add rustfmt >/dev/null 2>&1; cargo fmt --all"

cli-image: ## Build the valkvedge-cli image
	docker build --target cli -t valkvlabs/valkvedge-cli:dev .

down: ## Stop and remove the emulator and its data
	docker compose down -v
