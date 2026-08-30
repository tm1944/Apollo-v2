#!/usr/bin/env bash
set -euo pipefail

pod="$(kubectl -n apollo get pods -l app.kubernetes.io/name=worker -o jsonpath='{.items[0].metadata.name}')"
kubectl -n apollo delete pod "$pod" --wait=false
kubectl -n apollo rollout status deployment/worker --timeout=120s
printf 'Replaced worker pod %s\n' "$pod"
