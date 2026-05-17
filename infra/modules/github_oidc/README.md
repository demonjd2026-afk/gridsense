# Module: github_oidc

Wires GitHub Actions to Azure using **OIDC federation**. No client secrets
stored anywhere — GitHub presents a short-lived JWT, Azure validates it
against the federated credentials defined here, and issues an access token
scoped to this app registration.

## What gets created

- **Azure AD application** `<project>-github-actions-<env>`
- **Service principal** for the application
- **Three federated identity credentials**, each pinned to a different OIDC
  subject claim:
  - `repo:<org>/<repo>:pull_request` — PR-triggered workflows
  - `repo:<org>/<repo>:ref:refs/heads/main` — pushes to `main`
  - `repo:<org>/<repo>:environment:production` — reserved for future
    environment-gated workflows
- **RBAC role assignments**:
  - Contributor on the current subscription
  - Storage Blob Data Contributor on the tfstate storage account (passed in
    via `tfstate_storage_account_id`)

## Why three federated credentials, not one

Each GitHub Actions trigger produces a different OIDC `sub` claim. A wildcard
subject would defeat the purpose of the federation. Pinning three explicit
subjects gives least-privilege at the trigger level:

- PR workflows can only `terraform plan` (read-only on Azure)
- `main` workflows can `terraform apply` (write on Azure)
- The `production` environment is reserved for future manual-approval gating

## How GitHub Actions uses the output

In a workflow, after running this module's `terraform apply`:

```yaml
permissions:
  id-token: write   # ← REQUIRED for OIDC
  contents: read

steps:
  - uses: azure/login@v2
    with:
      client-id: ${{ secrets.AZURE_CLIENT_ID }}       # ← module output: client_id
      tenant-id: ${{ secrets.AZURE_TENANT_ID }}       # ← well-known
      subscription-id: ${{ secrets.AZURE_SUBSCRIPTION_ID }}
```

No `client-secret` field. No secret retrieved from Key Vault. The `id-token: write`
permission lets the runner request a JWT from GitHub's OIDC provider, which
Azure validates against the federated credential.

## Bootstrap order

This is a chicken-and-egg situation: the workflows need the SP to exist, but
the SP is created by Terraform that runs *in* the workflows. Solution:

1. Add this module to `infra/envs/dev/main.tf`
2. Run `terraform apply` **locally** (using your CLI auth) — this creates the
   SP and its federated credentials
3. Read the `client_id` output, add it (plus subscription_id, tenant_id) to
   GitHub Actions secrets
4. From this point on, all subsequent applies happen through GitHub Actions

## Permissions needed to apply this module

The executing identity needs:

- `Application.ReadWrite.OwnedBy` on the Azure AD tenant (for the app reg)
- `Owner` or `User Access Administrator` on the subscription (to assign
  Contributor to the new SP)
- `Owner` on the tfstate storage account (to assign Storage Blob Data
  Contributor)

For the local bootstrap, your personal CLI auth typically has all three
because you're the subscription owner.
