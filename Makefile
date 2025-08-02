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
	@echo "=== CRSL Demo ==="
	@echo "1. Creating sample content..."
	@CONTENT_ID=$$(cargo run --example cli -- create -c "This is a demo content" -a "demo-user" 2>/dev/null | grep "Content ID:" | awk '{print $$3}'); \
	echo "Created content with ID: $$CONTENT_ID"; \
	echo ""; \
	echo "2. Showing the created content:"; \
	cargo run --example cli -- show $$CONTENT_ID; \
	echo ""; \
	echo "3. Updating the content..."; \
	UPDATE_RESULT=$$(cargo run --example cli -- update -g $$CONTENT_ID -c "Updated demo content" -a "demo-user" 2>/dev/null); \
	echo "$$UPDATE_RESULT"; \
	echo ""; \
	echo "4. Showing the updated content:"; \
	cargo run --example cli -- show $$CONTENT_ID; \
	echo ""; \
	echo "=== Demo completed successfully! ==="; \
	echo "✓ Content was successfully created, updated, and displayed" 