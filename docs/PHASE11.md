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


## Verification — end-to-end CI/CD proven on 2026-05-17

![GitHub Actions workflow runs — three reds becoming green across Terraform Apply and Databricks Bundle Deploy](screenshots/phase11/phase11-1-actions-runs-history.png)

*GitHub Actions runs history. The mix of red and green is the actual debugging story —
three failures, each revealing a real permission gap, then a clean green run on the fourth attempt.*

![Successful Terraform Apply run showing OIDC login and all 6 steps green](screenshots/phase11/phase11-2-terraform-apply-detail.png)

*The successful Terraform Apply on commit `f1d6125`. Azure Login (OIDC) green, init green,
validate green, apply green. End-to-end OIDC authentication with zero stored secrets.*


After the local bootstrap apply, three CI runs proved each piece of the
trust chain. Two failed on real permission gaps; one passed clean.
Each failure exposed something genuine about how Azure separates
management-plane access from data-plane access, and the fixes are now
codified in the module.

### Run #1 (commit `aa093d4`) — failed

OIDC auth itself worked (Azure validated the JWT, issued an access token).
But the SP couldn't read Key Vault secrets:

```
Action: 'Microsoft.KeyVault/vaults/secrets/getSecret/action'
Caller: oid=3024593b-... (the SP)
Vault: kv-gridsense-dev-dx0kcg
Assignment: (not found)
```

Subscription Contributor grants Azure **management-plane** access (change
SKU, delete the vault) but does NOT grant **data-plane** access (read/write
secrets inside the vault). Same pattern as the tfstate storage account
needing `Storage Blob Data Contributor`, not just Contributor.

The SP also failed Azure AD application reads via Microsoft Graph:

```
ApplicationsClient.BaseClient.Get(): unexpected status 403
Authorization_RequestDenied: Insufficient privileges
```

Subscription Contributor grants Azure RBAC, not Microsoft Graph (Azure AD)
permissions. Reading existing `azuread_application` resources requires
`Application.Read.All` (or `ReadWrite.OwnedBy` for least privilege) on
Microsoft Graph.

### Fix #1 (commit `68d8919`)

Extended the `github_oidc` module with two new resources:

- `azurerm_role_assignment.key_vault_administrator` — Key Vault data plane
- `azuread_app_role_assignment.msgraph_application_readwrite_ownedby` —
  Microsoft Graph `Application.ReadWrite.OwnedBy`

Bootstrapped locally with `terraform apply`. SP now has 4 role assignments
total: Contributor (sub), Storage Blob Data Contributor (tfstate), Key Vault
Administrator (dev KV), and the Microsoft Graph app role.

### Run #2 (commit `68d8919`) — failed

OIDC + new permissions worked. But Terraform tried to *replace* the
foundation module's `kv_admin_user` role assignment:

```
principal_id = "3a075b46-..." -> "3024593b-..." # forces replacement
```

The resource used `data.azurerm_client_config.current.object_id` for its
`principal_id`, which floats based on who's running terraform. Locally it
resolves to the user's object_id; in CI it resolves to the SP's. The two
callers disagreed about desired state, so plan always wanted to replace.

The SP could create role assignments (Contributor allows that), but could
NOT delete a role assignment owned by the user (Azure RBAC anti-privilege-
escalation guardrail). So replace failed at the destroy step.

### Fix #2 (commit `f1d6125`)

Pinned `kv_admin_user.principal_id` to a stable variable rather than the
floating data source:

```hcl
variable "kv_admin_principal_id" { type = string }

resource "azurerm_role_assignment" "kv_admin_user" {
  principal_id = var.kv_admin_principal_id  # stable across callers
  ...
}
```

Wired from `infra/envs/dev/main.tf` to your user's object_id explicitly.
Both local and CI callers now see the same desired state. Replace loop
killed at the root cause.

This is strictly better than the original design — coupling
"who has Key Vault admin" to "who happened to run terraform last" was
never auditable or multi-user safe. Phase 11 forced surfacing of latent
debt.

### Run #3 (commit `f1d6125`) — Terraform Apply PASSED ✅

```
No changes. Your infrastructure matches the configuration.
Apply complete! Resources: 0 added, 0 changed, 0 destroyed.
```

The holy grail of idempotent CI. Plan and apply both clean.

### Databricks bundle deploy bring-up

The bundle-deploy workflow initially failed with:

```
Error: User not authorized. (403 UNKNOWN)
Endpoint: GET /api/2.0/preview/scim/v2/Me
```

The SP authenticated to Azure (no auth error), but the Databricks workspace
didn't recognize it as a workspace user. Databricks has its own permission
model layered on top of Azure RBAC — workspace identity is provisioned
separately from Azure RBAC because Databricks groups, cluster policies,
SQL warehouses, and notebook ACLs are all workspace-internal concepts.

### Fix — added SP to workspace (manual, one-time bootstrap)

The SP was already at Databricks account level (auto-created during the
first auth attempt). Added it to this specific workspace via the workspace
UI:

1. Workspace → Settings → Identity and access → Service principals
2. Click "Add service principal" → "Select existing service principal"
3. Pick `gridsense-github-actions-dev` from the account-level list
4. Add to workspace with default entitlements: Workspace access + Databricks
   SQL access

No admin role needed — minimum privilege was sufficient. The bundle deploy
runs as a regular workspace user; it doesn't need admin to deploy bundle
resources scoped to that user's home folder.

This is a one-time manual step. Documented here so anyone reproducing the
project does it once and forgets. A real team would do this via SCIM
provisioning from Azure AD; for a single-workspace dev environment, the
manual step is appropriate.

### Bundle deploy run — PASSED ✅

After the workspace grant, manual workflow_dispatch of the bundle-deploy
workflow:

```
Uploading bundle files to /Workspace/Users/.../bundle/gridsense/dev/files...
Deploying resources...
Updating deployment state...
Deployment complete!
```

End to end, in ~30 seconds. The full CI chain works.

## What this proves

Three claims that hold up under interview probing:

1. **No secrets at rest or in transit.** Every Azure access token in CI is
   minted fresh per workflow run and lasts ~10 minutes. The federated
   credential trust is encoded in Azure AD; nothing sensitive lives in
   GitHub repo secrets except the public-ish identifiers
   (client_id, tenant_id, subscription_id).

2. **Least privilege at the trigger level.** PR workflows can only auth for
   plan operations; main-branch workflows can apply. The same SP has
   different effective permissions depending on which OIDC subject claim
   GitHub presents.

3. **Idempotent end to end.** After all bring-up fixes, every subsequent
   CI run shows `0 added, 0 changed, 0 destroyed` unless the actual
   infrastructure or bundle code changes. Plan and apply converge.
