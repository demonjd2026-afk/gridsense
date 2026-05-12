# ============================================================================
# GridSense Makefile
# Daily-driver commands. Run `make help` to see what's available.
# ============================================================================

.PHONY: help fmt lint test deploy-dev destroy-dev producer-build providers-check azure-whoami clean

# Default environment for deploy/destroy targets. Override with: make deploy-dev ENV=staging
ENV ?= dev

# ----------------------------------------------------------------------------
# Help (default target)
# ----------------------------------------------------------------------------
help: ## Show this help message
	@echo ""
	@echo "GridSense - daily command reference"
	@echo "===================================="
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Examples:"
	@echo "  make fmt              # format all code"
	@echo "  make lint             # run linters"
	@echo "  make deploy-dev       # deploy infra + databricks to dev"
	@echo "  make destroy-dev      # tear down dev environment"
	@echo ""

# ----------------------------------------------------------------------------
# Code quality
# ----------------------------------------------------------------------------
fmt: ## Format all code (Terraform + Python)
	@echo ">>> Formatting Terraform..."
	terraform fmt -recursive infra/
	@echo ">>> Formatting Python..."
	uvx ruff format producers/ databricks/

lint: ## Lint all code (Terraform + Python)
	@echo ">>> Validating Terraform..."
	@for env in dev staging prod; do \
		if [ -f infra/envs/$$env/main.tf ]; then \
			echo "  - validating $$env"; \
			terraform -chdir=infra/envs/$$env init -backend=false > /dev/null; \
			terraform -chdir=infra/envs/$$env validate; \
		fi \
	done
	@echo ">>> Linting Python..."
	uvx ruff check producers/ databricks/

test: ## Run all unit tests
	@echo ">>> Running tests..."
	@for svc in producers/*/; do \
		if [ -f "$$svc/pyproject.toml" ]; then \
			echo "  - testing $$svc"; \
			cd $$svc && uv run pytest -q && cd -; \
		fi \
	done

# ----------------------------------------------------------------------------
# Deploy / destroy
# ----------------------------------------------------------------------------
deploy-dev: ## Deploy infra + Databricks bundle to dev
	@echo ">>> Deploying infrastructure to $(ENV)..."
	cd infra/envs/$(ENV) && terraform init && terraform apply -auto-approve
	@echo ">>> Deploying Databricks bundle to $(ENV)..."
	cd databricks && databricks bundle deploy -t $(ENV)
	@echo ">>> Deploy complete."

destroy-dev: ## Tear down the dev environment (costs stop accruing)
	@echo ">>> Destroying $(ENV) environment..."
	@read -p "Are you sure? Type 'destroy' to confirm: " confirm && \
	  [ "$$confirm" = "destroy" ] || (echo "Aborted." && exit 1)
	cd infra/envs/$(ENV) && terraform destroy -auto-approve
	@echo ">>> Destroyed. Verify in the Azure portal."

# ----------------------------------------------------------------------------
# Diagnostics
# ----------------------------------------------------------------------------
azure-whoami: ## Show current Azure CLI account
	@az account show --query "{subscription:name, id:id, user:user.name}" --output table

providers-check: ## Verify all required Azure resource providers are registered
	@echo ">>> Required providers status:"
	@for ns in Microsoft.Databricks Microsoft.EventHub Microsoft.Storage Microsoft.KeyVault \
	           Microsoft.App Microsoft.OperationalInsights Microsoft.ContainerRegistry \
	           Microsoft.CognitiveServices; do \
		state=$$(az provider show -n $$ns --query registrationState -o tsv 2>/dev/null); \
		printf "  %-35s %s\n" $$ns $$state; \
	done

# ----------------------------------------------------------------------------
# Housekeeping
# ----------------------------------------------------------------------------
clean: ## Remove local caches and build artifacts
	@echo ">>> Cleaning local caches..."
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .ruff_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .mypy_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .terraform -exec rm -rf {} + 2>/dev/null || true
	@echo ">>> Done."
