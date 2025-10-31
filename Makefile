.PHONY: help test fmt clippy check clean clean-data cli-init cli-create cli-update cli-show cli-history demo dev-setup

help:
	@echo "CRSL Development Commands:"
	@echo "  make test       - Run tests"
	@echo "  make fmt        - Format code"
	@echo "  make clippy     - Run clippy checks"
	@echo "  make check      - Format + clippy + test"
	@echo "  make clean      - Clean build artifacts"
	@echo "  make clean-data - Clean only data directories"
	@echo ""
	@echo "CLI Commands:"
	@echo "  make cli-init   - Initialize repository"
	@echo "  make cli-create - Create sample content"
	@echo "  make cli-update - Update content (requires GENESIS_ID)"
	@echo "  make cli-show   - Show content (requires ID)"
	@echo "  make cli-history - Show history from genesis (requires GENESIS_ID, optional MODE=linear)"
	@echo "  make demo       - Run complete demo workflow"
	@echo "  make dev-setup  - Setup development environment"

test:
	cargo test

fmt:
	cargo fmt

clippy:
	cargo clippy --workspace --all-targets --profile test --no-deps -- --deny warnings

check: fmt clippy test

clean:
	cargo clean
	rm -rf crsl_data/
	rm -rf test_db/

# Clean only data directories (faster than full clean)
clean-data:
	rm -rf crsl_data/
	rm -rf test_db/

# Default history mode for CLI targets (tree | linear)
MODE ?= tree

# CLI Commands
cli-init:
	cargo run --example cli -- init

cli-create:
ifndef CONTENT
	@echo "Creating sample content..."
	@echo "To create custom content, use: make cli-create CONTENT='Your content here'"
	cargo run --example cli -- create -c "Hello, CRSL!" -a "test-user"
else
	@echo "Creating content: $(CONTENT)"
	cargo run --example cli -- create -c "$(CONTENT)" -a "test-user"
endif

cli-update:
ifndef GENESIS_ID
	@echo "Error: GENESIS_ID is required"
	@echo "Usage: make cli-update GENESIS_ID=<id>"
	@echo ""
	@echo "Example:"
	@echo "  make cli-update GENESIS_ID=QmExample123"
	@exit 1
endif
	cargo run --example cli -- update -g $(GENESIS_ID) -c "Updated content" -a "test-user"

cli-show:
ifndef ID
	@echo "Error: ID is required"
	@echo "Usage: make cli-show ID=<content-id>"
	@echo ""
	@echo "Example:"
	@echo "  make cli-show ID=QmExample123"
	@exit 1
endif
	cargo run --example cli -- show $(ID)

cli-history:
ifndef GENESIS_ID
	@echo "Error: GENESIS_ID is required"
	@echo "Usage: make cli-history GENESIS_ID=<genesis-id>"
	@echo ""
	@echo "Optional: MODE=linear"
	@echo "Example:"
	@echo "  make cli-history GENESIS_ID=QmExample123 MODE=linear"
	@exit 1
endif
	cargo run --example cli -- history -g $(GENESIS_ID) --mode $(MODE)

# Development setup
dev-setup: cli-init cli-create
	@echo ""
	@echo "✓ Development environment setup complete!"
	@echo "Repository initialized and sample content created."
	@echo ""
	@echo "Next steps:"
	@echo "1. Run tests: make test"
	@echo "2. Try CLI: make cli-create"
	@echo "3. Run demo: make demo"

# Demo workflow
demo: clean-data cli-init
	@set -e; \
	echo ""; \
	echo "=== CRSL Version History Demo ==="; \
	echo "This demo seeds a branching storyline and inspects it from two perspectives."; \
	echo ""; \
	echo "📝 Step 1: Preparing sample history..."; \
	CONTENT_GENESIS=$$(cargo run --example cli -- create -c "Initial draft by Alice" -a "alice" 2>/dev/null | grep "Genesis:" | awk '{print $$2}'); \
	if [ -z "$$CONTENT_GENESIS" ]; then \
		echo "  ! Failed to create sample content"; \
		exit 1; \
	fi; \
	echo "  > Genesis CID: $$CONTENT_GENESIS"; \
	A1=$$(cargo run --example cli -- update -g $$CONTENT_GENESIS -c "Chapter A" -a "alice" 2>/dev/null | grep "Version" | awk '{print $$3}'); \
	echo "  > Alice adds Chapter A: $$A1"; \
	B1=$$(cargo run --example cli -- update -g $$CONTENT_GENESIS -c "Chapter B" -a "bob" 2>/dev/null | grep "Version" | awk '{print $$3}'); \
	echo "  > Bob adds Chapter B (linear): $$B1"; \
	B_BRANCH=$$(cargo run --example cli -- update -g $$CONTENT_GENESIS -c "Bob's branch revisited" -a "bob" --parent $$A1 2>/dev/null | grep "New Version" | awk '{print $$3}'); \
	echo "  > Bob branches from Chapter A: $$B_BRANCH"; \
	MERGE_TRIGGER=$$(cargo run --example cli -- update -g $$CONTENT_GENESIS -c "Merged storyline" -a "carol" 2>/dev/null | grep "Version" | awk '{print $$3}'); \
	echo "  > Carol pushes merge-friendly update: $$MERGE_TRIGGER"; \
	AUTO_MERGE=$$(cargo run --example cli -- history -g $$CONTENT_GENESIS --mode linear 2>/dev/null | grep "🔀" | awk '{print $$3}' | head -1); \
	if [ -n "$$AUTO_MERGE" ]; then \
		echo "  > 🤖 Auto-merge produced merge node: $$AUTO_MERGE"; \
	else \
		echo "  > ⚠️  Auto-merge node not detected"; \
	fi; \
	FINAL=$$(cargo run --example cli -- update -g $$CONTENT_GENESIS -c "Conclusion" -a "alice" 2>/dev/null | grep "Version" | awk '{print $$3}'); \
	echo "  > Alice writes conclusion: $$FINAL"; \
	echo ""; \
	echo "📊 Step 2: Inspecting history views..."; \
	echo ""; \
	echo "=== Branching History (node list) ==="; \
	cargo run --example cli -- history -g $$CONTENT_GENESIS; \
	echo ""; \
	echo "=== Linear Timeline (version list) ==="; \
	cargo run --example cli -- history -g $$CONTENT_GENESIS --mode linear; \
	echo ""; \
	echo "=== Version CIDs ==="; \
	VERSION_LIST=$$(cargo run --example cli -- history -g $$CONTENT_GENESIS --mode linear 2>/dev/null | grep -E "🌱|🧩|🔀|✨" | awk '{print $$3}'); \
	echo "$$VERSION_LIST" | tr ' ' '\n'; \
	echo ""; \
	echo "=== Demo completed successfully! ==="; \
	echo "✓ Sample storyline created"; \
	echo "✓ Branching nodes and ordered versions displayed"; \
	echo ""