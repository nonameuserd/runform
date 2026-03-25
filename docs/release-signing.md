## Release signing (CI)

This repo’s release workflow (`.github/workflows/release.yml`) publishes Nuitka-built standalone executables for macOS/Windows/Linux.

- If signing secrets are **not** configured, builds are still produced and uploaded, but are **unsigned / not notarized**.
- If signing secrets **are** configured, CI will:
  - macOS: **codesign** the produced `bin/akc` binary inside the standalone bundle (Developer ID) and **notarize** a zip of that bundle with `notarytool`
  - Windows: sign `akc.exe` using **`signtool.exe`** with an RFC3161 timestamp

### Checksums

Each uploaded archive has a sibling `*.sha256` file, and CI also publishes a combined `SHA256SUMS.txt` asset for the release (covers all Nuitka archives).

### Optional: Cosign bundles (sigstore)

CI can optionally create **Cosign bundle files** (`*.sigstore.bundle.json`) for each release archive (Cosign “sign-blob” bundles).

Enable one of the following:

- **Keyless (recommended)**: set repository variable **`COSIGN_KEYLESS=true`**.
  - Requires GitHub Actions OIDC (the workflow requests `id-token: write`).
- **Key-based**: create secrets **`COSIGN_PRIVATE_KEY`** (and optionally `COSIGN_PASSWORD` if your key is encrypted).

### GitHub Secrets

Create these secrets in GitHub: **Settings → Secrets and variables → Actions → New repository secret**.

#### macOS: Developer ID Application + notarization (notarytool API key)

- **`APPLE_SIGNING_CERT_P12_BASE64`**
  - Base64 encoding of a `.p12` containing your **Developer ID Application** certificate *and private key*.
  - Create from Keychain by exporting the certificate as `.p12`, then:

```bash
base64 -i developer_id_application.p12 | pbcopy
```

- **`APPLE_SIGNING_CERT_P12_PASSWORD`**
  - The password you used when exporting the `.p12`.

- **`APPLE_SIGNING_IDENTITY`**
  - The codesign identity label, for example:
    - `Developer ID Application: Your Org (TEAMID)`

- **`APPLE_NOTARYTOOL_KEY_ID`**
  - App Store Connect API key id (e.g. `ABC123DEFG`).

- **`APPLE_NOTARYTOOL_ISSUER_ID`**
  - App Store Connect issuer id (UUID).

- **`APPLE_NOTARYTOOL_PRIVATE_KEY_P8_BASE64`**
  - Base64 encoding of the App Store Connect API key private key `AuthKey_<KEY_ID>.p8`, for example:

```bash
base64 -i "AuthKey_${KEY_ID}.p8" | pbcopy
```

#### Windows: Authenticode signing (signtool)

- **`WINDOWS_SIGNING_CERT_PFX_BASE64`**
  - Base64 encoding of a `.pfx` code signing certificate *with private key*:

```bash
certutil -encode -f codesign.pfx codesign.pfx.b64
```

  - Then paste the contents of `codesign.pfx.b64` (without the BEGIN/END header lines) into the secret.

- **`WINDOWS_SIGNING_CERT_PFX_PASSWORD`**
  - The password for the `.pfx`.

### Notes and limitations

- **Notarization format**: CI notarizes a **zip of the standalone bundle directory** (as built by Nuitka). If you later switch distribution to `.pkg`/`.dmg`, update notarization to submit that installer artifact instead.
- **Verification**:
  - macOS: `codesign --verify` is run in CI; `spctl` output is logged as best-effort.
  - Windows: `signtool verify /pa /v` is run in CI.

