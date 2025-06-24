#!/bin/sh
set -eu

# Fallbacks
BIN_FOLDER="${BIN_FOLDER:-${HOME}/.local/bin}"

# Computing artifact location
case "$(uname)" in
  Linux)
    PLATFORM="linux" ;;
  Darwin)
    PLATFORM="osx" ;;
  *NT*)
    PLATFORM="win" ;;
esac

ARCH="$(uname -m)"
case "$ARCH" in
  aarch64|ppc64le|arm64)
      ;;  # pass
  *)
    ARCH="64" ;;
esac

case "$PLATFORM-$ARCH" in
  linux-aarch64|linux-ppc64le|linux-64|osx-arm64|osx-64|win-64)
      ;;  # pass
  *)
    echo "Failed to detect your OS" >&2
    exit 1
    ;;
esac

if [ "${VERSION:-}" = "" ]; then
  RELEASE_URL="https://github.com/mamba-org/micromamba-releases/releases/latest/download/micromamba-${PLATFORM}-${ARCH}"
else
  RELEASE_URL="https://github.com/mamba-org/micromamba-releases/releases/download/${VERSION}/micromamba-${PLATFORM}-${ARCH}"
fi


# Downloading artifact
mkdir -p "${BIN_FOLDER}"
if hash curl >/dev/null 2>&1; then
  curl "${RELEASE_URL}" -o "${BIN_FOLDER}/micromamba" -fsSL --compressed ${CURL_OPTS:-}
elif hash wget >/dev/null 2>&1; then
  wget ${WGET_OPTS:-} -qO "${BIN_FOLDER}/micromamba" "${RELEASE_URL}"
else
  echo "Neither curl nor wget was found" >&2
  exit 1
fi
chmod +x "${BIN_FOLDER}/micromamba"


# Extract expected checksum
EXE_NAME="micromamba-${PLATFORM}-${ARCH}"
EXPECTED_HASH="$(cut -d' ' -f1 < "./checksums/$EXE_NAME.sha256")"

# Calculate actual checksum
if command -v sha256sum >/dev/null 2>&1; then
    ACTUAL_HASH="$(sha256sum "$TEMP_FILE" | cut -d' ' -f1)"
elif command -v shasum >/dev/null 2>&1; then
    ACTUAL_HASH="$(shasum -a 256 "$TEMP_FILE" | cut -d' ' -f1)"
else
    echo "error: no suitable SHA256 tool found (need sha256sum or shasum)" >&2
    exit 1
fi

# Compare checksums
if [ "$EXPECTED_HASH" != "$ACTUAL_HASH" ]; then
    echo "error: SHA256 checksum mismatch!" >&2
    echo "Expected: $EXPECTED_HASH" >&2
    echo "Actual:   $ACTUAL_HASH" >&2
    exit 1
fi