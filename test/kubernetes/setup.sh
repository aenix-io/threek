#!/usr/bin/env bash
# Prepares a kind cluster for the Kubernetes end-to-end tests: cert-manager,
# etcd-operator and the threek policy injecting AGENT_IMAGE, which is loaded
# into the nodes. NODE_IMAGE, if set, picks the kind node image.
#
# Usage: test/kubernetes/setup.sh <kind cluster name> <agent image>
set -o errexit -o nounset -o pipefail

cluster=$1
agent_image=$2
cert_manager_version=v1.21.2
operator_version=v0.5.8
context="kind-${cluster}"
here=$(cd "$(dirname "$0")" && pwd)
kind=${KIND:-kind}

node_image=()
if [[ -n "${NODE_IMAGE:-}" ]]; then
  node_image=(--image "${NODE_IMAGE}")
fi
"${kind}" create cluster --name "${cluster}" --config "${here}/kind.yaml" "${node_image[@]}" --wait 2m
"${kind}" load docker-image --name "${cluster}" "${agent_image}"

kubectl --context "${context}" apply --server-side \
  --filename "https://github.com/cert-manager/cert-manager/releases/download/${cert_manager_version}/cert-manager.yaml"
kubectl --context "${context}" wait --namespace cert-manager --for=condition=Available --timeout=5m deployment --all

kubectl --context "${context}" apply --server-side \
  --filename "https://github.com/cozystack/etcd-operator/releases/download/${operator_version}/etcd-operator.yaml"
kubectl --context "${context}" wait --namespace etcd-operator-system --for=condition=Available --timeout=5m deployment --all

sed "s|\"ghcr.io/aenix-io/threek-agent:[^\"]*\"|\"${agent_image}\"|" "${here}/../../deploy/threek-agent.yaml" \
  | kubectl --context "${context}" apply --filename -
