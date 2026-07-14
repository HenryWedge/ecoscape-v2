# Ecoscape – Edge Computing Testbed Ideen

Ecoscape als Plattform für Edge-Computing-Experimente auf Kubernetes.
Dieses Dokument skizziert Erweiterungen, die reale Edge-Szenarien
simulierbar machen.

---

## 1. Node Resource Profiles ("Edge Device Templates")

Ein `edgeProfiles`-Feld im Experiment-Spec beschreibt Edge-Geräte
mit ihren begrenzten Ressourcen (CPU, RAM, Speicher, Netzwerk).

```yaml
spec:
  edgeProfiles:
    - name: raspberry-pi-4
      cpu: "4"
      memory: "8Gi"
      ephemeralStorage: "32Gi"
      networkLatency: "50ms"
      bandwidth: "100Mbps"
      nodeSelector:
        ecoscape.cau-se.de/edge-node: "true"
```

**Umsetzung:**
- Controller deployt `stress-ng`-DaemonSet auf markierten Edge-Nodes
- Setzt CPU/Memory-Limits via Linux cgroups
- Injiziert Netzwerk-Latenz/Bandbreite via `tc` (traffic control) oder chaos-mesh
- Prometheus-Metriken messen, ob Workloads innerhalb der Limits performen

**SLO-Beispiel:**
```yaml
slos:
  - name: edge-response-time
    query: 'histogram_quantile(0.95, rate(request_duration_seconds_bucket{zone="edge"}[1m]))'
    threshold: 500
    thresholdDirection: LessThanOrEqual
```

---

## 2. Edge Zones + Netzwerk-Topologie

Eine `edgeZones`-Spezifikation modelliert geografisch verteilte Standorte
mit realistischen Netzwerkparametern zwischen den Zonen.

```yaml
spec:
  edgeZones:
    - name: berlin
      nodeSelector:
        topology.kubernetes.io/zone: berlin
      network:
        uplinkLatency: "30ms"
        uplinkBandwidth: "50Mbps"
        jitter: "5ms"
    - name: munich
      nodeSelector:
        topology.kubernetes.io/zone: munich
      network:
        uplinkLatency: "45ms"
        uplinkBandwidth: "20Mbps"
  interZoneLinks:
    - from: berlin
      to: munich
      latency: "15ms"
      bandwidth: "100Mbps"
    - from: berlin
      to: cloud
      latency: "30ms"
      bandwidth: "50Mbps"
```

**Umsetzung:**
- Controller erstellt pro Zone ein Namespace-Label
- Injiziert Netzwerk-Chaos zwischen Zonen via chaos-mesh `NetworkChaos`
- Oder: `iptables`/`tc`-Init-Container in den Workloads
- Erlaubt Experimentaufbau wie: "App läuft auf 3 Edge-Zones + Cloud-Fallback"

---

## 3. Intermittent Connectivity ("Disconnect Schedule")

Edge-Geräte gehen regelmäßig offline (IoT, Mobilfunk, Batteriesparmodus).
Ein Zeitplan definiert Online/Offline-Phasen.

```yaml
spec:
  connectivity:
    - zone: berlin
      pattern:
        online: "45s"
        offline: "15s"
        jitter: "5s"
    - zone: munich
      pattern:
        online: "120s"
        offline: "30s"
```

**Umsetzung:**
- Controller startet einen Goroutine pro Zone mit dem angegebenen Takt
- In der Offline-Phase: Pods per NetworkPolicy isolieren oder Chaos-Mesh `PodChaos` nutzen
- Metriken: Queue-Größen, Retry-Raten, Datenverlust während Offline-Phasen
- Testet Resilienz-Muster wie: Offline-First, CQRS, Event Sourcing

---

## 4. Workload Placement Policies ("Offloading")

Automatisiert testen, wie sich Workloads bei verschiedenen
Scheduling-Strategien verhalten – von "nur Edge" bis "Cloud-Fallback".

```yaml
spec:
  placement:
    strategy: latency-optimized
    edgeOnly: false
    fallbackToCloud: true
    cloudNodeSelector:
      topology.kubernetes.io/zone: cloud
```

**Umsetzung:**
- Controller generiert PodTemplates mit passenden:
  - `NodeAffinity` für Edge/Cloud-Nodes
  - `Tolerations` für Taints
  - `TopologySpreadConstraints` für Multi-Zone
- Unterstützte Strategien:
  - `edge-only` – Workloads dürfen nur auf Edge-Nodes laufen
  - `latency-optimized` – nähestmöglicher Node zum Client
  - `resource-aware` – Cloud wird genutzt, wenn Edge-Ressourcen knapp sind
  - `battery-aware` – vermeidet rechenintensive Tasks auf Battery-Geräten
- SLOs messen Latenz, Cost und Durchsatz pro Strategie

---

## 5. Power / Energy Modelling

Edge-Geräte haben oft begrenzte Energie (Batterie, Solar).
Ein Power-Modell simuliert den Energieverbrauch des Workloads.

```yaml
spec:
  powerProfiles:
    - name: battery-5000mah
      capacity: 5000
      drainPerCPUPercent: 5.0
      drainPerMBMemory: 0.1
      drainPerMbpsNetwork: 0.5
```

**Umsetzung:**
- Controller berechnet simulierten Verbrauch aus CPU/Memory/Network-Nutzung
- Exportiert Metrik `ecoscape_battery_level` in Prometheus
- SLO: `battery_runtime > experiment_duration` (reicht der Akku?)
- Später: Integration mit echten Power-Metriken via `kepler` oder `powercap`
- Ermöglicht Experimente wie: "Wie oft muss ein Sensor nachladen bei 5s Intervall?"

---

## 6. Data Gravity / Data Locality

Edge-Geräte erzeugen Daten lokal – nicht alle Daten können in die Cloud.

```yaml
spec:
  dataLocality:
    - zone: berlin
      localStorage: "50Gi"
      syncToCloud: false
      syncBatchInterval: "60s"
    - zone: munich
      localStorage: "100Gi"
      syncToCloud: true
```

**Umsetzung:**
- Controller deployt lokale PVs (oder hostPath) auf Edge-Nodes
- Simuliert Datenverlust bei Offline-Phasen oder Node-Failures
- Metriken: Sync-Dauer, Datenverlust, lokaler Storage-Verbrauch
- Testet Data-Locality-Konzepte: Write-behind Cache, CRDTs, Gossip-Protokolle

## 7. Chaos Mesh als Infrastruktur-Definition (ersetzt "Infra-Phase")

Chaos Mesh bietet 12+ Chaos-Typen, mit denen sich reale
Edge-Infrastruktur-Szenarien beschreiben lassen. Das aktuelle
"infra"-Feld (YAML-Manifeste) wird durch eine deklarative
Infrastruktur-Spezifikation ersetzt, die direkt auf Chaos Mesh CRDs
abbildet.

```yaml
spec:
  infrastructure:
    topology:
      zones:
        - name: berlin
          nodeSelector:
            topology.kubernetes.io/zone: berlin
          network:
            uplinkLatency: "30ms"
            uplinkBandwidth: "50Mbps"
        - name: munich
          nodeSelector:
            topology.kubernetes.io/zone: munich
          network:
            uplinkLatency: "45ms"
            uplinkBandwidth: "20Mbps"
    conditions:
      - name: edge-device-stress
        schedule: "during-measurement"
        chaos:
          type: StressChaos
          spec:
            stressors:
              cpu:
                workers: 2
                load: 80
              memory:
                workers: 1
                size: "512MB"
      - name: mobile-network-loss
        schedule: "0/30 * * * *"
        duration: "5s"
        chaos:
          type: NetworkChaos
          spec:
            action: loss
            loss:
              loss: "25"
              correlation: "50"
```

### Chaos Mesh Typen und ihre Edge-Entsprechung

#### 7.1 PodChaos – Node-/Device-Ausfälle

| Chaos Mesh | Edge-Szenario |
|---|---|
| `pod-kill` | Edge-Device stürzt ab (Battery leer, Überhitzung) |
| `container-kill` | Einzelner Service auf dem Edge-Gerät crasht |
| `pod-failure` | Node ist unerreichbar (Netzwerkausfall, Hardware-Defekt) |

```yaml
conditions:
  - name: random-edge-crash
    schedule: "@every 5m"
    duration: "30s"
    chaos:
      type: PodChaos
      spec:
        action: pod-failure
        mode: one
        selector:
          namespaces: ["edge-zone-berlin"]
```

#### 7.2 NetworkChaos – Edge-Netzwerk-Realitäten

Das wichtigste Feature für Edge-Simulation:

| Chaos Mesh | Edge-Szenario |
|---|---|
| `delay` | Latenz via Mobilfunk (LTE: 20-100ms, 5G: 10-30ms, Satellit: 250-600ms) |
| `loss` | Packet Loss bei schlechtem Empfang (2-30%) |
| `duplicate` | Duplikate bei Mesh-Netzwerken |
| `corrupt` | Bitfehler bei Funkstörungen |
| `partition` | Komplette Netzwerk-Trennung (Edge offline) |
| `bandwidth` | Bandbreiten-Limit (IoT: 100kbps, LTE: 10Mbps) |

```yaml
conditions:
  - name: satellite-link
    schedule: "during-measurement"
    chaos:
      type: NetworkChaos
      spec:
        action: delay
        delay:
          latency: "550ms"
          jitter: "50ms"
          correlation: "75"
  - name: lte-congestion
    schedule: "during-load"
    chaos:
      type: NetworkChaos
      spec:
        action: bandwidth
        rate: "5mbps"
        limit: 10000
        buffer: 1000
```

#### 7.3 StressChaos – Ressourcen-Limits auf Edge Devices

Simuliert überlastete oder schwache Edge-Hardware:

| Chaos Mesh | Edge-Szenario |
|---|---|
| `cpu-stress` | CPU-Drosselung bei Überhitzung |
| `memory-stress` | Memory-Pressure, OOM-Szenarien |
| `mixed-stress` | Gesamtlast auf schwacher Hardware (Pi, Jetson) |

```yaml
conditions:
  - name: raspberry-pi-cpu-throttle
    schedule: "during-measurement"
    chaos:
      type: StressChaos
      spec:
        stressors:
          cpu:
            workers: 4
            load: 60
          memory:
            workers: 2
            size: "256MB"
```

#### 7.4 IOChaos – Storage-Simulation für SD-Karten & Flash

Edge-Geräte nutzen oft langsame SD-Karten oder eMMC:

| Chaos Mesh | Edge-Szenario |
|---|---|
| `latency` | SD-Karte mit langsamen I/O |
| `fault` | Dateisystem-Fehler, Read-only-Filesystem |
| `attrOverride` | Permissions-Probleme |

```yaml
conditions:
  - name: sd-card-latency
    schedule: "during-measurement"
    chaos:
      type: IOChaos
      spec:
        action: latency
        delay: "100ms"
        volumePath: "/data"
        methods:
          - read
          - write
```

#### 7.5 DNSChaos – DNS-Probleme im Edge-Umfeld

Edge-Geräte haben oft instabile DNS-Verbindungen:

| Chaos Mesh | Edge-Szenario |
|---|---|
| `error` | DNS-Server nicht erreichbar |
| `random` | Zufällige DNS-Fehler |

```yaml
conditions:
  - name: dns-failure
    schedule: "@every 2m"
    duration: "10s"
    chaos:
      type: DNSChaos
      spec:
        action: error
        patterns:
          - "*"
```

#### 7.6 HTTPChaos – Service-Fehlverhalten auf Edge APIs

Edge-Services antworten oft langsam oder fehlerhaft:

| Chaos Mesh | Edge-Szenario |
|---|---|
| `delay` | Langsame Edge-API (schlechte Anbindung) |
| `abort` | Verbindung abbruch (Timeout) |
| `replace` | Fehlerhafte Antworten (falsche Daten) |

```yaml
conditions:
  - name: slow-edge-api
    schedule: "during-measurement"
    chaos:
      type: HTTPChaos
      spec:
        mode: all
        target: Request
        port: 8080
        delay:
          delay: "2s"
          percent: 50
```

#### 7.7 TimeChaos – Clock Skew auf IoT-Geräten

Edge-Geräte haben oft keine RTC oder driftende Uhren:

| Chaos Mesh | Edge-Szenario |
|---|---|
| `time-skew` | Clock Drift (bis zu Stunden bei schlechter NTP-Sync) |

```yaml
conditions:
  - name: clock-drift
    schedule: "during-measurement"
    chaos:
      type: TimeChaos
      spec:
        mode: all
        timeOffset: "5m"
```

#### 7.8 KernelChaos – System-Level Störungen

Für Hardware-nahe Szenarien:

| Chaos Mesh | Edge-Szenario |
|---|---|
| `kernel-fault` | Kernel Panic (Absturz) |
| `fs-fault` | Corruptes Filesystem (SD-Karte defekt) |

### Schedule-Modi für Conditions

Inspiriert von der Edge-Realität (nicht alle Probleme treten
dauerhaft auf):

| Modus | Beschreibung |
|---|---|
| `during-measurement` | Aktiv während der gesamten Measurement-Phase |
| `during-load` | Nur während der Load-Phase (realistische Last-Spitzen) |
| `during-chaos` | Chaos-Phase (zusätzliche Störungen) |
| `during-repetition:N` | Nur in Wiederholung N |
| `<cron>` | Cron-Ausdruck (z.B. `0/30 * * * * *`) |
| `<duration>/<interval>` | Zyklisch (z.B. `10s/30s` = 10s Chaos alle 30s) |

### Kombinierte Szenarien (Condition Groups)

Komplexe Edge-Infrastruktur durch gleichzeitige Conditions:

```yaml
conditions:
  - name: schlechter-mobilfunk-verbindung
    schedule: "during-measurement"
    chaos:
      type: NetworkChaos
      spec:
        action: delay
        delay:
          latency: "100ms"
          jitter: "30ms"
  - name: packet-loss-dazu
    schedule: "during-measurement"
    chaos:
      type: NetworkChaos
      spec:
        action: loss
        loss:
          loss: "5"
  - name: cpu-ueberlast-gleichzeitig
    schedule: "during-measurement"
    chaos:
      type: StressChaos
      spec:
        stressors:
          cpu:
            workers: 2
            load: 70
```

Ersetzt das manuelle Bereitstellen von Chaos-Manifesten in der
Infra-Phase – der Controller übersetzt die Infrastructure- und
Conditions-Spezifikation automatisch in Chaos Mesh CRDs,
steuert deren Lebenszyklus (Start/Stopp zum richtigen Zeitpunkt)
und räumt sie nach dem Experiment wieder auf.

---

## Priorisierung

| # | Idee | Aufwand | Erkenntnisgewinn | Abhängigkeiten |
|---|------|---------|-----------------|----------------|
| 1 | **Chaos Mesh Infrastruktur-Definition** | gering | sehr hoch | Chaos Mesh installiert |
| 2 | Node Resource Profiles | mittel | hoch | Chaos Mesh oder tc |
| 3 | Edge Zones + Netzwerk | mittel | hoch | Chaos Mesh, Multi-Node-Cluster |
| 4 | Intermittent Connectivity | gering | hoch | Chaos Mesh |
| 5 | Placement Policies | hoch | sehr hoch | Scheduling-Know-how |
| 6 | Power Modelling | gering | mittel | Metriken-Export, kepler |
| 7 | Data Gravity | hoch | mittel | PV-Provisioning |

**Empfehlung:** 1 → 4 → 3 → 2 → 5 → 6 → 7
