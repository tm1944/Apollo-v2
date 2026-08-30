#!/usr/bin/env bash
set -euo pipefail

kubectl -n apollo delete pod apollo-healthcheck --ignore-not-found
kubectl -n apollo run apollo-healthcheck \
  --image=apollo-control-plane:dev \
  --image-pull-policy=IfNotPresent \
  --restart=Never \
  --command -- /healthcheck -address control-plane:50051
kubectl -n apollo wait --for=jsonpath='{.status.phase}'=Succeeded pod/apollo-healthcheck --timeout=30s
kubectl -n apollo logs apollo-healthcheck
kubectl -n apollo delete pod apollo-healthcheck
