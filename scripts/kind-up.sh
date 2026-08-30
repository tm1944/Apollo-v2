#!/usr/bin/env bash
set -euo pipefail

cluster_name="${APOLLO_KIND_CLUSTER:-apollo}"

if ! kind get clusters | grep -Fxq "$cluster_name"; then
  kind create cluster --name "$cluster_name" --config deploy/k8s/kind-config.yaml
fi

docker build -f control-plane/Dockerfile -t apollo-control-plane:dev .
docker build -f worker/Dockerfile -t apollo-worker:dev .
kind load docker-image --name "$cluster_name" apollo-control-plane:dev apollo-worker:dev
kubectl apply -k deploy
kubectl -n apollo rollout status deployment/control-plane --timeout=120s
kubectl -n apollo rollout status deployment/worker --timeout=120s
kubectl -n apollo rollout status deployment/prometheus --timeout=120s
kubectl -n apollo rollout status deployment/grafana --timeout=120s
kubectl -n apollo get pods
