# Everything runs in Docker. No local Rust toolchain is needed.
IMAGE  ?= rust:1-bookworm
RUN     = docker run --rm -v "$(CURDIR)":/src -w /src \
          -v vke-cargo:/usr/local/cargo/registry -v vke-target:/target \
          -e CARGO_TARGET_DIR=/target -e CARGO_TERM_COLOR=always $(IMAGE)

.PHONY: help up unit integ lint fmt down cli-image shell bucket hooks

help: ## List the commands
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-10s %s\n", $$1, $$2}'

up: ## Start the S3 emulator and create its test bucket
	@docker volume create vke-cargo >/dev/null && docker volume create vke-target >/dev/null
	docker compose up -d --wait s3
	docker compose run --rm s3-init

unit: ## Unit and in-process tests (no external services)
	$(RUN) cargo test --workspace --locked

integ: ## Integration tests against the S3 emulator
	@docker volume create vke-cargo >/dev/null && docker volume create vke-target >/dev/null
	docker compose up -d --wait s3
	docker compose run --rm tests

lint: ## Format check and clippy, warnings as errors
	$(RUN) sh -c "rustup component add rustfmt clippy >/dev/null 2>&1; cargo fmt --all --check && cargo clippy --workspace --all-targets --locked -- -D warnings"

fmt: ## Format the code
	$(RUN) sh -c "rustup component add rustfmt >/dev/null 2>&1; cargo fmt --all"

cli-image: ## Build the valkvedge-cli image
	docker build --target cli -t valkvlabs/valkvedge-cli:dev .

down: ## Stop and remove the emulator and its data
	docker compose down -v

shell: ## A shell with the S3 emulator configured, for trying things by hand
	@docker volume create vke-cargo >/dev/null && docker volume create vke-target >/dev/null
	docker compose up -d --wait s3
	docker compose run --rm tests bash

bucket: ## List every object the emulator holds
	docker compose exec s3 awslocal s3 ls --recursive s3://valkvedge-test

hooks: ## Install the git hooks (run the checks before every push)
	git config core.hooksPath .githooks
	@echo "hooks installed: lint and tests now run before each push"
