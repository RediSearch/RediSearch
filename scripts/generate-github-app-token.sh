#!/bin/bash
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1).

# Generate a GitHub App installation access token using bash/openssl.
# This is used on platforms that don't support node20 (e.g., amazonlinux:2).
#
# Required environment variables:
#   GH_APP_ID         - GitHub App ID
#   GH_APP_PRIVATE_KEY - GitHub App private key (PEM format)
#
# Optional environment variables:
#   GITHUB_OUTPUT     - If set, outputs token to GitHub Actions output
#
# Output:
#   Prints the token to stdout (last line)
#   If GITHUB_OUTPUT is set, also writes to GitHub Actions output as 'token'

set -e

if [ -z "$GH_APP_ID" ]; then
    echo "Error: GH_APP_ID environment variable is required" >&2
    exit 1
fi

if [ -z "$GH_APP_PRIVATE_KEY" ]; then
    echo "Error: GH_APP_PRIVATE_KEY environment variable is required" >&2
    exit 1
fi

# Generate JWT using openssl (from GitHub docs)
now=$(date +%s)
iat=$((now - 60))
exp=$((now + 600))

b64enc() { openssl base64 | tr -d '=' | tr '/+' '_-' | tr -d '\n'; }

header='{"typ":"JWT","alg":"RS256"}'
header_b64=$(echo -n "$header" | b64enc)

payload="{\"iat\":${iat},\"exp\":${exp},\"iss\":\"${GH_APP_ID}\"}"
payload_b64=$(echo -n "$payload" | b64enc)

unsigned="${header_b64}.${payload_b64}"
signature=$(echo -n "$unsigned" | openssl dgst -sha256 -sign <(echo "$GH_APP_PRIVATE_KEY") | b64enc)

JWT="${unsigned}.${signature}"

# Get installation ID for the org
echo "Fetching installation ID for redislabsdev org..." >&2
INSTALL_RESPONSE=$(curl -s \
    -H "Authorization: Bearer $JWT" \
    -H "Accept: application/vnd.github+json" \
    https://api.github.com/orgs/redislabsdev/installation)
INSTALLATION_ID=$(echo "$INSTALL_RESPONSE" | jq -r '.id')

if [ -z "$INSTALLATION_ID" ] || [ "$INSTALLATION_ID" = "null" ]; then
    echo "Error: Failed to get installation ID. Response: $INSTALL_RESPONSE" >&2
    exit 1
fi
echo "Installation ID: $INSTALLATION_ID" >&2

# Get installation access token
echo "Generating installation access token..." >&2
TOKEN_RESPONSE=$(curl -s -X POST \
    -H "Authorization: Bearer $JWT" \
    -H "Accept: application/vnd.github+json" \
    "https://api.github.com/app/installations/${INSTALLATION_ID}/access_tokens" \
    -d '{"repositories":["Redis","RediSearchEnterprise","rust-speedb","speedb-ent","redismodule-rs"]}')
TOKEN=$(echo "$TOKEN_RESPONSE" | jq -r '.token')

if [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
    echo "Error: Failed to get access token. Response: $TOKEN_RESPONSE" >&2
    exit 1
fi
echo "Token generated successfully" >&2

# Output token
if [ -n "$GITHUB_OUTPUT" ]; then
    # GitHub Actions: mask the token and write to GITHUB_OUTPUT
    echo "::add-mask::$TOKEN"
    echo "token=$TOKEN" >> "$GITHUB_OUTPUT"
else
    # Non-GitHub Actions: print to stdout for capturing in variable
    echo "$TOKEN"
fi

