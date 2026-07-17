#!/usr/bin/env bash
# install.sh — installiert Prometheus + Chaos Mesh via Helm in Minikube
# Aufruf: bash sut/dependencies/install.sh
# Voraussetzung: helm und kubectl sind installiert, minikube läuft

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Farben für Ausgabe
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()    { echo -e "${GREEN}[INFO]${NC} $*"; }
warning() { echo -e "${YELLOW}[WARN]${NC} $*"; }

# ── Voraussetzungen prüfen ──────────────────────────────────────────────────
info "Prüfe Voraussetzungen..."
for cmd in helm kubectl; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "Fehler: '$cmd' ist nicht installiert." >&2
    exit 1
  fi
done

if ! kubectl cluster-info &>/dev/null; then
  echo "Fehler: Kein erreichbarer Kubernetes-Cluster (läuft minikube?)" >&2
  exit 1
fi

# ── Helm Repos ─────────────────────────────────────────────────────────────
info "Helm Repos hinzufügen..."
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add chaos-mesh https://charts.chaos-mesh.org
helm repo update

# ── Prometheus (kube-prometheus-stack) ─────────────────────────────────────
info "Namespace 'monitoring' anlegen..."
kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -

info "Installiere kube-prometheus-stack..."
helm upgrade --install prometheus prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --values "${SCRIPT_DIR}/prometheus/values.yaml" \
  --wait \
  --timeout 5m

info "Prometheus läuft. UI erreichbar mit:"
echo "  kubectl port-forward -n monitoring svc/prometheus-operated 9090:9090"
echo "  -> http://localhost:9090"

# ── Chaos Mesh ─────────────────────────────────────────────────────────────
info "Namespace 'chaos-mesh' anlegen..."
kubectl create namespace chaos-mesh --dry-run=client -o yaml | kubectl apply -f -

info "Installiere Chaos Mesh..."
helm upgrade --install chaos-mesh chaos-mesh/chaos-mesh \
  --namespace chaos-mesh \
  --values "${SCRIPT_DIR}/chaos-mesh/values.yaml" \
  --wait \
  --timeout 5m

info "Chaos Mesh läuft."

# ── Statusübersicht ────────────────────────────────────────────────────────
echo ""
info "Alle Pods:"
kubectl get pods -n monitoring
echo ""
kubectl get pods -n chaos-mesh
echo ""
info "Fertig. Alle Dependencies sind installiert."
