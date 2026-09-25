import json
import sys
import numpy as np
import pandas as pd

# matplotlib may be installed in a non-default location; adjust path if needed
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
except ModuleNotFoundError:
    print("ERROR: matplotlib not found. Install via: pip install matplotlib")
    sys.exit(1)

# ==============================================================================
# KONFIGURATION
# ==============================================================================
FILE_PATH        = "result.jsonl"
SLO_GROUP_1      = "stream-lag-berlin"
SLO_GROUP_2      = "stream-lag-munich"
PLOT_OUTPUT      = "chaos_effect.pdf"
SECONDS_PER_STEP = 5   # each measurement index represents 5 seconds


# ==============================================================================
# HILFSFUNKTIONEN
# ==============================================================================
def load_data(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        records = [json.loads(line) for line in f if line.strip()]
    return pd.DataFrame(records)


# ==============================================================================
# 1. RELATIVE EFFEKTGRÖSSE (FOLD CHANGE δ): (Chaos - Pre) / Pre pro Repetition
# ==============================================================================
def relative_effect(df, slo_name, label):
    print(f"  {label} ({slo_name})")
    deltas = []
    for rep in sorted(df["repetition"].unique()):
        pre   = df[(df["slo"] == slo_name) & (df["phase"] == "PreChaosMeasurement") & (df["repetition"] == rep)]["value"]
        chaos = df[(df["slo"] == slo_name) & (df["phase"] == "ChaosMeasurement")    & (df["repetition"] == rep)]["value"]
        if pre.empty or chaos.empty:
            continue
        mu_pre, mu_chaos = pre.mean(), chaos.mean()
        delta = (mu_chaos - mu_pre) / mu_pre
        deltas.append(delta)
        print(f"    Rep {rep:2d} | Pre={mu_pre:.2f}  Chaos={mu_chaos:.2f}  δ={delta:+.3f}")

    deltas = np.array(deltas)
    print(f"  → mean(δ) = {deltas.mean():+.3f}   std(δ) = {deltas.std():.3f}   "
          f"min={deltas.min():+.3f}   max={deltas.max():+.3f}\n")
    return deltas


# ==============================================================================
# 2. ABSOLUTE DISPERSION auf gemeinsamer (globaler) Skala
# ==============================================================================
def absolute_dispersion(df):
    chaos = df[df["phase"] == "ChaosMeasurement"]
    g_min = chaos["value"].min()
    g_max = chaos["value"].max()
    print(f"  Globale Skala (ChaosMeasurement): [{g_min}, {g_max}]")

    rows = []
    for slo_name, label in [(SLO_GROUP_1, "Z1"), (SLO_GROUP_2, "Z2")]:
        rep_means = (
            chaos[chaos["slo"] == slo_name]
            .groupby("repetition")["value"]
            .mean()
        )
        rep_means_norm = (rep_means - g_min) / (g_max - g_min)
        rows.append({
            "Gruppe":          f"{label} ({slo_name})",
            "mean (normiert)": round(rep_means_norm.mean(), 4),
            "std  (normiert)": round(rep_means_norm.std(),  4),
            "CV":              round(rep_means_norm.std() / rep_means_norm.mean(), 3),
            "min (normiert)":  round(rep_means_norm.min(), 4),
            "max (normiert)":  round(rep_means_norm.max(), 4),
        })

    print(pd.DataFrame(rows).to_string(index=False))
    print()


# ==============================================================================
# 3. PEARSON KORRELATION (Median der Chaos-Phase über Messzeitpunkte)
# ==============================================================================
def pearson_correlation_chaos_median(df):
    chaos = df[df["phase"] == "ChaosMeasurement"]

    def median_per_index(slo_name):
        grouped = chaos[chaos["slo"] == slo_name].groupby("index")["value"]
        indices = sorted(grouped.groups.keys())
        return np.array([grouped.get_group(i).median() for i in indices])

    med1 = median_per_index(SLO_GROUP_1)
    med2 = median_per_index(SLO_GROUP_2)

    n = min(len(med1), len(med2))
    med1, med2 = med1[:n], med2[:n]

    # Pearson r via numpy (no scipy dependency)
    r = float(np.corrcoef(med1, med2)[0, 1])

    # Two-tailed p-value via t-distribution approximation
    t_stat = r * np.sqrt((n - 2) / (1 - r**2))
    from math import lgamma, pi as PI
    def t_cdf_upper(t, df):
        """Approximate P(T > |t|) for two-tailed test using regularised incomplete beta."""
        x = df / (df + t * t)
        # regularised incomplete beta via continued fraction (simple series)
        # For large df a normal approx is fine; use scipy-free formula
        import math
        # Use the relation: p = I_x(df/2, 0.5) via the log-beta function
        # For practical purposes: use the Abramowitz & Stegun normal approximation
        z = abs(t) * (1 - 1/(4*df)) / np.sqrt(1 + t*t/(2*df))
        p_one = 0.5 * (1 + math.erf(z / math.sqrt(2)))
        return 2 * (1 - p_one)

    p = t_cdf_upper(t_stat, n - 2)

    print(f"  Pearson r = {r:+.4f}   p-value ≈ {p:.4e}   n = {n}")
    print()
    return r, p


# ==============================================================================
# 3b. PEARSON KORRELATION innerhalb einer Zone (paarweise zwischen 8 Läufen)
#     Jede Repetition liefert eine Zeitreihe: Median je Messzeitpunkt (index)
#     in der Chaos-Phase. Alle Paare werden verglichen → 8×8-Matrix.
# ==============================================================================
def pearson_correlation_within_zone(df, slo_name, label):
    """
    Pro Repetition: Zeitreihe der Mediane je Messzeitpunkt (index) in der
    Chaos-Phase. Paarweise Pearson-Korrelation zwischen allen 8 Läufen.
    Gibt Korrelationsmatrix aus sowie Mean / Std / Min / Max aller Paare.
    """
    import math

    chaos = df[(df["phase"] == "ChaosMeasurement") & (df["slo"] == slo_name)]
    reps  = sorted(chaos["repetition"].unique())

    # Zeitreihe pro Repetition: Median je index
    series = {}
    for rep in reps:
        sub     = chaos[chaos["repetition"] == rep]
        grouped = sub.groupby("index")["value"]
        indices = sorted(grouped.groups.keys())
        series[rep] = np.array([grouped.get_group(i).median() for i in indices])

    # Alle Vektoren auf gemeinsame Länge kürzen
    min_len = min(len(v) for v in series.values())
    for rep in reps:
        series[rep] = series[rep][:min_len]

    n_reps = len(reps)

    # Pearson r + p-Wert für ein Paar
    def pearson_p(a, b):
        n  = len(a)
        r  = float(np.corrcoef(a, b)[0, 1])
        if n < 3 or abs(r) >= 1.0:
            return r, float("nan")
        t_stat = r * math.sqrt((n - 2) / (1 - r ** 2))
        dof    = n - 2
        z      = abs(t_stat) * (1 - 1 / (4 * dof)) / math.sqrt(1 + t_stat ** 2 / (2 * dof))
        p      = 2 * (1 - 0.5 * (1 + math.erf(z / math.sqrt(2))))
        return r, p

    # Korrelationsmatrix berechnen
    r_matrix = np.full((n_reps, n_reps), np.nan)
    p_matrix = np.full((n_reps, n_reps), np.nan)
    for i, ri in enumerate(reps):
        for j, rj in enumerate(reps):
            if i == j:
                r_matrix[i, j] = 1.0
                p_matrix[i, j] = 0.0
            elif j > i:
                r, p = pearson_p(series[ri], series[rj])
                r_matrix[i, j] = r_matrix[j, i] = r
                p_matrix[i, j] = p_matrix[j, i] = p

    # Ausgabe Korrelationsmatrix (r-Werte)
    header = "  " + "".join(f"  Rep{rep:>2}" for rep in reps)
    print(f"  Korrelationsmatrix r  [{label} – {slo_name}]")
    print(header)
    for i, rep in enumerate(reps):
        row = "".join(f"  {r_matrix[i, j]:+.3f}" for j in range(n_reps))
        print(f"  Rep{rep:>2}{row}")

    # Nur oberes Dreieck (eindeutige Paare)
    upper = [r_matrix[i, j] for i in range(n_reps) for j in range(i + 1, n_reps)]
    upper = np.array(upper)

    print()
    print(f"  Paarweise r  (n={len(upper)} Paare):")
    print(f"    Mean = {upper.mean():+.4f}   Std = {upper.std():.4f}   "
          f"Min = {upper.min():+.4f}   Max = {upper.max():+.4f}")

    if   upper.mean() >= 0.9: interp = "sehr starke"
    elif upper.mean() >= 0.7: interp = "starke"
    elif upper.mean() >= 0.5: interp = "moderate"
    elif upper.mean() >= 0.3: interp = "schwache"
    else:                     interp = "keine/vernachlässigbare"
    print(f"  → im Mittel {interp} {'positive' if upper.mean() >= 0 else 'negative'} Korrelation")
    print()
    return r_matrix, p_matrix


# ==============================================================================
# 4. PLOT
# ==============================================================================
def compute_band_stats(df, slo_name, phase):
    """Per measurement-index: median, mean±std, min, max across repetitions."""
    subset  = df[(df["slo"] == slo_name) & (df["phase"] == phase)]
    grouped = subset.groupby("index")["value"]
    indices = sorted(grouped.groups.keys())
    median  = np.array([grouped.get_group(i).median() for i in indices])
    mean    = np.array([grouped.get_group(i).mean()   for i in indices])
    std     = np.array([grouped.get_group(i).std()    for i in indices])
    vmin    = np.array([grouped.get_group(i).min()    for i in indices])
    vmax    = np.array([grouped.get_group(i).max()    for i in indices])
    return np.array(indices), median, mean - std, mean + std, vmin, vmax


def plot_bands(ax, time_s, median, std_lo, std_hi, vmin, vmax, color):
    """Draw min/max, ±std bands and median line onto ax."""
    ax.fill_between(time_s, vmin,   vmax,   color=(*color[:3], 0.12), linewidth=0)
    ax.fill_between(time_s, std_lo, std_hi, color=(*color[:3], 0.30), linewidth=0)
    ax.plot(time_s, median, color=color, linewidth=2.0)


def build_time_series(df, slo_name):
    """
    Concatenate PreChaos + Chaos phases into a single time axis (seconds).
    Pre:   index 0..n_pre-1  → time 0 .. (n_pre-1)*5
    Chaos: index 0..n_chaos-1 → time n_pre*5 .. (n_pre+n_chaos-1)*5
    Returns (pre_times, pre_stats, chaos_times, chaos_stats, separator_time)
    """
    pre_idx, *pre_stats   = compute_band_stats(df, slo_name, "PreChaosMeasurement")
    chaos_idx, *chaos_stats = compute_band_stats(df, slo_name, "ChaosMeasurement")

    n_pre        = len(pre_idx)
    pre_times    = pre_idx * SECONDS_PER_STEP
    # Chaos starts immediately at the last pre time point (no gap)
    chaos_times  = pre_times[-1] + chaos_idx * SECONDS_PER_STEP
    separator_t  = pre_times[-1]   # x position of phase boundary

    return pre_times, pre_stats, chaos_times, chaos_stats, separator_t


def style_ax(ax, separator_t, x_max):
    """Apply common axis styling and phase separator."""
    ax.axvline(separator_t, color="0.45", linewidth=1.2,
               linestyle="--", zorder=3)
    ax.set_xlabel("Experiment Duration (s)", fontsize=20)
    ax.set_ylabel("Replication Lag (s)", fontsize=20)
    ax.tick_params(labelsize=20)
    ax.set_xlim(0, x_max)
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", color="0.88", linewidth=0.6, zorder=0)
    ax.set_axisbelow(True)
    ax.spines[["top", "right"]].set_visible(False)


def make_legend_handles(color, label):
    c = color[:3]
    return [
        mpatches.Patch(color=(*c, 0.12), label=f"{label} – Min / Max"),
        mpatches.Patch(color=(*c, 0.30), label=f"{label} – Mean ± Std"),
        plt.Line2D([0], [0], color=color, linewidth=2, label=f"{label} – Median"),
    ]


def draw_panel(ax, df, slo_name, title, color):
    pre_t, pre_stats, chaos_t, chaos_stats, sep_t = build_time_series(df, slo_name)

    plot_bands(ax, pre_t,   *pre_stats,   color)
    plot_bands(ax, chaos_t, *chaos_stats, color)

    x_max = chaos_t[-1]
    style_ax(ax, sep_t, x_max)
    ax.set_title(title, fontsize=20, fontweight="bold", pad=8)

    c = color[:3]
    handles = [
        mpatches.Patch(color=(*c, 0.12), label="Min / Max"),
        mpatches.Patch(color=(*c, 0.30), label="Mean ± Std"),
        plt.Line2D([0], [0], color=color, linewidth=2, label="Median"),
    ]
    ax.legend(handles=handles, fontsize=20, loc="upper left",
              framealpha=0.7, bbox_to_anchor=(0.01, 0.99))


def generate_plot(df, output_path):
    """Two stacked panels, one per SLO."""
    fig, (ax_zone2, ax_zone1) = plt.subplots(
        2, 1, figsize=(8.27, 11.69),
        constrained_layout=True,
    )
    draw_panel(ax_zone2, df, SLO_GROUP_2,
               title="stream-lag-zone2",
               color=(0.80, 0.18, 0.08, 1.0))
    draw_panel(ax_zone1, df, SLO_GROUP_1,
               title="stream-lag-zone1",
               color=(0.10, 0.35, 0.75, 1.0))

    fig.savefig(output_path, format="pdf", bbox_inches="tight")
    plt.close(fig)
    print(f"  Plot saved to: {output_path}")


def generate_combined_plot(df, output_path):
    """Both SLOs in one diagram with a single shared y-axis."""
    COLOR_MUNICH = (0.80, 0.18, 0.08, 1.0)
    COLOR_BERLIN = (0.10, 0.35, 0.75, 1.0)

    fig, ax = plt.subplots(figsize=(14, 3.5), constrained_layout=True)

    sep_t = None
    x_max = None
    for slo, color in [(SLO_GROUP_2, COLOR_MUNICH), (SLO_GROUP_1, COLOR_BERLIN)]:
        pre_t, pre_stats, chaos_t, chaos_stats, sep_t = build_time_series(df, slo)
        plot_bands(ax, pre_t,   *pre_stats,   color)
        plot_bands(ax, chaos_t, *chaos_stats, color)
        x_max = chaos_t[-1]

    style_ax(ax, sep_t, x_max)
    legend_handles = (
        make_legend_handles(COLOR_BERLIN, "Zone 1") +
        make_legend_handles(COLOR_MUNICH, "Zone 2")
    )

    ax.legend(handles=legend_handles, fontsize=11, loc="upper left",
              framealpha=0.85, ncol=2)

    fig.savefig(output_path, format="pdf", bbox_inches="tight")
    plt.close(fig)
    print(f"  Combined plot saved to: {output_path}")


# ==============================================================================
# MAIN
# ==============================================================================
if __name__ == "__main__":
    try:
        df = load_data(FILE_PATH)

        print("=" * 70)
        print("1. RELATIVE EFFEKTGRÖSSE  δ = (Chaos - Pre) / Pre  pro Repetition")
        print("=" * 70 + "\n")
        for slo, label in [(SLO_GROUP_1, "Z1"), (SLO_GROUP_2, "Z2")]:
            relative_effect(df, slo, label)

        print("=" * 70)
        print("2. ABSOLUTE DISPERSION auf globaler Skala (Repetitions-Mittelwerte)")
        print("=" * 70 + "\n")
        absolute_dispersion(df)

        print("=" * 70)
        print("3. PEARSON KORRELATION  (Median der Chaos-Phase, pro Zeitpunkt)")
        print("=" * 70 + "\n")
        pearson_correlation_chaos_median(df)

        print("=" * 70)
        print("3b. PEARSON KORRELATION  (paarweise zwischen 8 Läufen, pro Zone)")
        print("=" * 70 + "\n")
        pearson_correlation_within_zone(df, SLO_GROUP_1, "Z1")
        pearson_correlation_within_zone(df, SLO_GROUP_2, "Z2")

        print("=" * 70)
        print("4. PLOT")
        print("=" * 70 + "\n")
        generate_plot(df, PLOT_OUTPUT)
        generate_combined_plot(df, PLOT_OUTPUT.replace(".pdf", "_combined.pdf"))
    except FileNotFoundError:
        print(f"Datei '{FILE_PATH}' nicht gefunden.")
