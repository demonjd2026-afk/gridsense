# Phase 11 — CI/CD with OIDC Federation

GitHub Actions workflows for `terraform plan/apply` and `databricks bundle
deploy`, authenticated to Azure via **OpenID Connect federation** rather
than stored client secrets.

## Why OIDC, not a client secret

The 2020-era pattern for GitHub Actions → Azure was:

1. Create a service principal with a client secret
2. Store the secret as `AZURE_CLIENT_SECRET` in GitHub repo secrets
3. Hope no one leaks the secret

The 2026-standard pattern is:

1. Create a service principal with **no credentials at all**
2. Configure a **federated credential** trust between GitHub's OIDC provider
   and the SP, scoped to a specific repo + branch/PR/environment
3. On every workflow run, GitHub mints a short-lived JWT specifically for
   that run. The JWT's `sub` claim identifies the exact trigger
   (`repo:org/repo:ref:refs/heads/main`, for example).
4. The runner exchanges that JWT for an Azure access token via the federated
   credential. The token is scoped, short-lived, and never persisted.

No client secret ever exists. There is nothing for an attacker to exfiltrate.

## What got built

### Terraform module — `infra/modules/github_oidc/`

Creates the Azure-side trust:

- App registration `gridsense-github-actions-dev`
- Service principal
- Three federated credentials (PR, main-branch, production-environment)
- RBAC: Contributor on subscription + Storage Blob Data Contributor on
  the tfstate storage account

Module call lives at the bottom of `infra/envs/dev/main.tf`. Outputs the
new SP's `client_id`, which goes into GitHub repo secrets.

### Three GitHub Actions workflows — `.github/workflows/`

| File | Trigger | Permissions |
|---|---|---|
| `terraform-plan.yml` | PR with changes under `infra/**` | Plans only, posts plan as PR comment |
| `terraform-apply.yml` | Push to `main` (or manual) under `infra/**` | Applies infra changes |
| `databricks-bundle-deploy.yml` | Push to `main` (or manual) under `databricks/**` | Deploys notebooks + job YAMLs to the workspace |

All three use `azure/login@v2` with `client-id`, `tenant-id`,
`subscription-id` — no client-secret field. The runners receive
`ARM_USE_OIDC=true` and Terraform's azurerm provider does the JWT exchange
under the hood.

The plan workflow uses a less-privileged trigger (PR, read-only Azure
operations) than the apply workflow (push to main, write operations).
Mapping different triggers to different federated credentials gives
least-privilege at the CI level.

### README badge

A green/red CI badge at the top of `README.md` linking to the workflow
runs page. Quick visual signal that the project is in a working state.

## Bootstrap sequence

CI/CD with OIDC has a chicken-and-egg problem: the workflows need the SP
to exist, but the SP is created by Terraform that the workflows run.
Solution: one-time local bootstrap.

```bash
# 1. From the repo root, locally
cd infra/envs/dev
terraform init
terraform apply   # this creates the SP + federated credentials

# 2. Read the client_id from the apply output
terraform output github_actions_client_id

# 3. Add to GitHub repo secrets (Settings → Secrets and variables → Actions)
#    - AZURE_CLIENT_ID         (from step 2)
#    - AZURE_TENANT_ID         (e7bebb5c-49ff-4ef2-9ea8-b15b636c0ea1)
#    - AZURE_SUBSCRIPTION_ID   (1262ba1e-e555-43f6-a5a6-d61c2c3abf3b)

# 4. From now on, every PR and push runs in GitHub Actions
```

From step 4 onward, you can technically `terraform destroy` your local
state directory and never run anything locally again — all CRUD goes
through CI.

## What this demonstrates beyond the technical work

- **Modern auth**. OIDC federation with no client secrets is what 2026
  recruiters want to hear; "we use a stored client secret in CI" is now
  a yellow flag.
- **Least privilege at the trigger level**. The PR workflow can read but
  not write Azure; the main-branch workflow can write. This is rarely
  done well even at companies that use OIDC.
- **End-to-end deployment**. Terraform manages infra; Asset Bundles
  manage notebook+job code; both deploy from the same CI on the same
  trigger. No manual `databricks bundle deploy` from a laptop.

## Follow-ups

- **Environment-gated production deploy.** The third federated credential
  (`subject = ...:environment:production`) is reserved for a future
  workflow that requires manual approval in a GitHub Environment. Today
  there is no prod, so this is dormant.
- **Tighter RBAC**. The subscription-scoped Contributor role is broad on
  purpose for a portfolio project. In a real environment, replace with
  resource-group-scoped Contributor + specific role assignments for
  Databricks workspace, Event Hubs Data Owner, etc.
- **Bundle deploy permissions**. The Databricks workspace inherits the
  SP's Contributor role from the subscription scope. If a finer-grained
  Databricks workspace permission model is wanted later, add an explicit
  workspace-level `Can Manage` grant for the SP.
