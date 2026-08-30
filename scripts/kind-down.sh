#!/usr/bin/env bash
set -euo pipefail

kind delete cluster --name "${APOLLO_KIND_CLUSTER:-apollo}"
