#!/usr/bin/env sh
set -eu

usage() {
  cat <<'EOF'
Install the AKC CLI from GitHub Releases (prebuilt binary).

Usage:
  curl -fsSL https://raw.githubusercontent.com/nonameuserd/runform/main/scripts/install-akc.sh | sh

Options (as env vars):
  AKC_VERSION        Release tag to install (e.g. v0.2.0). Default: latest
  AKC_GITHUB_REPO    GitHub repo in owner/name form. Default: nonameuserd/runform
  AKC_INSTALL_DIR    Install dir for the 'akc' executable. Default: ~/.local/bin

Examples:
  AKC_VERSION=v0.1.0 sh install-akc.sh
  AKC_INSTALL_DIR=/usr/local/bin sh install-akc.sh
EOF
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  usage
  exit 0
fi

repo="${AKC_GITHUB_REPO:-nonameuserd/runform}"
tag="${AKC_VERSION:-}"
install_dir="${AKC_INSTALL_DIR:-$HOME/.local/bin}"

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1" >&2; exit 1; }
}

need_cmd uname
need_cmd mktemp
need_cmd rm
need_cmd mkdir
need_cmd chmod
need_cmd curl
need_cmd tar
need_cmd unzip

os="$(uname -s)"
arch="$(uname -m)"

case "$os" in
  Darwin) os_id="macos" ;;
  Linux) os_id="linux" ;;
  *)
    echo "Unsupported OS: $os" >&2
    echo "Tip: use PyPI install instead: pipx install akc" >&2
    exit 1
    ;;
esac

case "$arch" in
  arm64|aarch64) arch_id="arm64" ;;
  x86_64|amd64) arch_id="x64" ;;
  *)
    echo "Unsupported architecture: $arch" >&2
    exit 1
    ;;
esac

platform="${os_id}_${arch_id}"

api_base="https://api.github.com/repos/${repo}/releases"
if [ -n "$tag" ]; then
  rel_url="${api_base}/tags/${tag}"
else
  rel_url="${api_base}/latest"
fi

tmp="$(mktemp -d)"
cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

release_json="${tmp}/release.json"
curl -fsSL "$rel_url" -o "$release_json"

pick_asset_url() {
  # Avoid jq dependency: minimal JSON scraping for "browser_download_url".
  # We search for a matching archive name and then take the next download URL.
  asset_name="$1"
  awk -v name="\"name\": \"${asset_name}\"" -v url='"browser_download_url": "' '
    $0 ~ name { found=1; next }
    found && $0 ~ url {
      gsub(/.*"browser_download_url": "/, "", $0)
      gsub(/".*/, "", $0)
      print $0
      exit
    }
  ' "$release_json"
}

tag_name="$(awk -F'"' '/"tag_name":/ { print $4; exit }' "$release_json")"
if [ -z "${tag_name:-}" ]; then
  echo "Failed to determine release tag (bad GitHub API response?)" >&2
  exit 1
fi

tar_asset="akc_${tag_name}_${platform}.tar.gz"
zip_asset="akc_${tag_name}_${platform}.zip"

asset_url="$(pick_asset_url "$tar_asset" || true)"
asset_kind="tar.gz"
asset_file="$tar_asset"
if [ -z "$asset_url" ]; then
  asset_url="$(pick_asset_url "$zip_asset" || true)"
  asset_kind="zip"
  asset_file="$zip_asset"
fi
if [ -z "$asset_url" ]; then
  echo "No prebuilt asset found for platform ${platform} in release ${tag_name}" >&2
  echo "Tip: use PyPI install instead: pipx install akc" >&2
  exit 1
fi

archive_path="${tmp}/${asset_file}"
curl -fsSL "$asset_url" -o "$archive_path"

extract_dir="${tmp}/extract"
mkdir -p "$extract_dir"
case "$asset_kind" in
  tar.gz) tar -xzf "$archive_path" -C "$extract_dir" ;;
  zip) unzip -q "$archive_path" -d "$extract_dir" ;;
  *) echo "Unsupported archive kind: $asset_kind" >&2; exit 1 ;;
esac

# Archives are produced from the Nuitka bundle dir (akc.dist) under:
#   akc_<tag>_<platform>/akc.bin   (macOS/Linux)
#   akc_<tag>_<platform>/akc.exe   (Windows; not supported by this script)
bin_path="$(find "$extract_dir" -type f -name 'akc.bin' -print | head -n 1 || true)"
if [ -z "$bin_path" ]; then
  echo "Extracted archive did not contain akc.bin (unexpected release asset format)" >&2
  exit 1
fi

mkdir -p "$install_dir"
out="${install_dir%/}/akc"
cp "$bin_path" "$out"
chmod 0755 "$out"

echo "Installed: $out"
echo "Version:  $tag_name"
echo "Next:     ensure '${install_dir%/}' is on PATH, then run 'akc --help'"
