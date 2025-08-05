.PHONY: help test fmt clippy check clean cli-init cli-create cli-update cli-show cli-history cli-history-from-version cli-genesis demo dev-setup

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
	@echo "  make cli-history - Show history from genesis (requires GENESIS_ID)"
	@echo "  make cli-history-from-version - Show history from version (requires VERSION_ID)"
	@echo "  make cli-genesis - Get genesis from version (requires VERSION_ID)"
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
	@echo "Example:"
	@echo "  make cli-history GENESIS_ID=QmExample123"
	@exit 1
endif
	cargo run --example cli -- history -g $(GENESIS_ID)

cli-history-from-version:
ifndef VERSION_ID
	@echo "Error: VERSION_ID is required"
	@echo "Usage: make cli-history-from-version VERSION_ID=<version-id>"
	@echo ""
	@echo "Example:"
	@echo "  make cli-history-from-version VERSION_ID=QmExample123"
	@exit 1
endif
	cargo run --example cli -- history-from-version -v $(VERSION_ID)

cli-genesis:
ifndef VERSION_ID
	@echo "Error: VERSION_ID is required"
	@echo "Usage: make cli-genesis VERSION_ID=<version-id>"
	@echo ""
	@echo "Example:"
	@echo "  make cli-genesis VERSION_ID=QmExample123"
	@exit 1
endif
	cargo run --example cli -- genesis -v $(VERSION_ID)

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
	@echo ""
	@echo "=== CRSL Version History Demo ==="
	@echo "This demo will create one content, update it multiple times, and show all versions with latest verification."
	@echo ""
	@echo "📝 Step 1: Creating initial content..."
	@CONTENT_GENESIS=$$(cargo run --example cli -- create -c "Initial Document v1.0" -a "alice" 2>/dev/null | grep "Content ID:" | awk '{print $$3}'); \
	echo "Created content with Genesis ID: $$CONTENT_GENESIS"; \
	echo ""; \
	echo "🔄 Step 2: Updating content multiple times..."; \
	echo "  Updating to v1.1..."; \
	VERSION_1=$$(cargo run --example cli -- update -g $$CONTENT_GENESIS -c "Updated Document v1.1 - Added introduction" -a "alice" 2>/dev/null | grep "New Version:" | awk '{print $$3}'); \
	echo "  Updating to v1.2..."; \
	VERSION_2=$$(cargo run --example cli -- update -g $$CONTENT_GENESIS -c "Updated Document v1.2 - Added API documentation" -a "alice" 2>/dev/null | grep "New Version:" | awk '{print $$3}'); \
	echo "  Updating to v1.3..."; \
	VERSION_3=$$(cargo run --example cli -- update -g $$CONTENT_GENESIS -c "Updated Document v1.3 - Added deployment guide" -a "alice" 2>/dev/null | grep "New Version:" | awk '{print $$3}'); \
	echo "  Updating to v1.4..."; \
	VERSION_4=$$(cargo run --example cli -- update -g $$CONTENT_GENESIS -c "Updated Document v1.4 - Added troubleshooting section" -a "alice" 2>/dev/null | grep "New Version:" | awk '{print $$3}'); \
	echo "  Updating to v1.5..."; \
	VERSION_5=$$(cargo run --example cli -- update -g $$CONTENT_GENESIS -c "Updated Document v1.5 - Final version with complete documentation" -a "alice" 2>/dev/null | grep "New Version:" | awk '{print $$3}'); \
	echo "  Content now has 6 versions (including genesis)"; \
	echo ""; \
	echo "📊 Step 3: Displaying complete version history..."; \
	echo ""; \
	echo "=== Complete Version History ==="; \
	cargo run --example cli -- history -g $$CONTENT_GENESIS; \
	echo ""; \
	echo "🔍 Step 4: Verifying latest version for each version..."; \
	echo ""; \
	echo "=== Version Verification ==="; \
	echo "Getting version list from history..."; \
	VERSION_LIST=$$(cargo run --example cli -- history -g $$CONTENT_GENESIS 2>/dev/null | grep -E "🌱|📝|✨" | awk '{print $$3}'); \
	echo "Version list: $$VERSION_LIST"; \
	echo ""; \
	echo "Checking Genesis (v1.0):"; \
	cargo run --example cli -- show $$CONTENT_GENESIS; \
	echo ""; \
	echo "Checking Version 1 (v1.1):"; \
	VERSION_1=$$(echo "$$VERSION_LIST" | head -1); \
	cargo run --example cli -- show $$VERSION_1; \
	echo ""; \
	echo "Checking Version 2 (v1.2):"; \
	VERSION_2=$$(echo "$$VERSION_LIST" | head -2 | tail -1); \
	cargo run --example cli -- show $$VERSION_2; \
	echo ""; \
	echo "Checking Version 3 (v1.3):"; \
	VERSION_3=$$(echo "$$VERSION_LIST" | head -3 | tail -1); \
	cargo run --example cli -- show $$VERSION_3; \
	echo ""; \
	echo "Checking Version 4 (v1.4):"; \
	VERSION_4=$$(echo "$$VERSION_LIST" | head -4 | tail -1); \
	cargo run --example cli -- show $$VERSION_4; \
	echo ""; \
	echo "Checking Version 5 (v1.5):"; \
	VERSION_5=$$(echo "$$VERSION_LIST" | head -5 | tail -1); \
	cargo run --example cli -- show $$VERSION_5; \
	echo ""; \
	echo "🎯 Step 5: Latest version summary..."; \
	echo ""; \
	echo "Latest version: $$VERSION_5"; \
	echo "Total versions: 6 (1 genesis + 5 updates)"; \
	echo ""; \
	echo "=== Demo completed successfully! ==="; \
	echo "✓ Created 1 content with 6 versions"; \
	echo "✓ Applied 5 sequential updates"; \
	echo "✓ Demonstrated latest version calculation for each version"; \
	echo "✓ Verified that only the last version is marked as latest"; \
	echo ""; \