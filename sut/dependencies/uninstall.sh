#!/usr/bin/env bash
# uninstall.sh — entfernt Prometheus + Chaos Mesh vollständig
# Aufruf: bash sut/dependencies/uninstall.sh

set -euo pipefail

GREEN='\033[0;32m'
NC='\033[0m'
info() { echo -e "${GREEN}[INFO]${NC} $*"; }

info "Deinstalliere kube-prometheus-stack..."
helm uninstall prometheus --namespace monitoring 2>/dev/null || true

info "Deinstalliere Chaos Mesh..."
helm uninstall chaos-mesh --namespace chaos-mesh 2>/dev/null || true

info "Lösche CRDs (Chaos Mesh)..."
kubectl get crd | grep chaos-mesh.org | awk '{print $1}' | xargs -r kubectl delete crd 2>/dev/null || true

info "Lösche CRDs (Prometheus Operator)..."
kubectl get crd | grep monitoring.coreos.com | awk '{print $1}' | xargs -r kubectl delete crd 2>/dev/null || true

info "Lösche Namespaces..."
kubectl delete namespace monitoring --ignore-not-found
kubectl delete namespace chaos-mesh --ignore-not-found

info "Fertig."
