#!/usr/bin/env python3
"""Regenerate the benchmark plots embedded in the README and docs/benchmarks/.

Reads the committed result JSONs/CSV under docs/benchmarks/ and writes PNGs to
docs/img/. Deterministic: same inputs -> same images.

Usage:  python scripts/plots/make_all.py
"""

import csv
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[2]
BENCH = ROOT / "docs" / "benchmarks"
OUT = ROOT / "docs" / "img"

STYLE = {
    "baseline": dict(color="#888888", marker="o", linestyle="--"),
    "warm": dict(color="#1f77b4", marker="o", linestyle="-"),
}


def _load_points(*json_names: str) -> list[dict]:
    """Merge per-prefix-length result points from one or more benchmark JSONs."""
    points = []
    for name in json_names:
        data = json.loads((BENCH / name).read_text())
        for model in data["models"]:
            for r in model["results"]:
                points.append(
                    {
                        "model": model["model"],
                        "tokens": r["prompt_tokens"],
                        "baseline_p50": r["baseline_ms"]["p50"],
                        "warm_p50": r["external_warm_ms"]["p50"],
                        "delta_pct": r["warm_vs_baseline_pct"],
                    }
                )
    points.sort(key=lambda p: (p["model"], p["tokens"]))
    return points


def ttft_crossover_l4() -> None:
    """AWS g6.2xlarge (1x L4) TTFT crossover: the headline result.

    Cache loses at trivial prefixes, breaks even ~1k tokens, wins +10.9% @ 4k.
    """
    pts = _load_points(
        "phase45-distributed-qwen7b.json", "phase45-distributed-qwen7b-long.json"
    )
    x = [p["tokens"] for p in pts]

    fig, (ax, ax2) = plt.subplots(
        2, 1, figsize=(7.5, 6), sharex=True, height_ratios=[3, 1.4]
    )
    ax.plot(x, [p["baseline_p50"] for p in pts], label="vLLM baseline (no external cache)", **STYLE["baseline"])
    ax.plot(x, [p["warm_p50"] for p in pts], label="distributed cache, warm hit", **STYLE["warm"])
    ax.set_ylabel("TTFT p50 (ms)")
    ax.set_title(
        "TTFT crossover — Qwen2.5-7B on AWS g6.2xlarge (1× L4), cross-AZ cache"
    )
    ax.legend()
    ax.grid(True, alpha=0.3)

    deltas = [p["delta_pct"] for p in pts]
    colors = ["#2ca02c" if d > 0 else "#d62728" for d in deltas]
    ax2.bar(x, deltas, width=[max(40, t * 0.12) for t in x], color=colors, alpha=0.8)
    ax2.axhline(0, color="black", linewidth=0.8)
    ax2.set_ylabel("warm vs baseline (%)")
    ax2.set_xlabel("shared-prefix length (tokens)")
    ax2.grid(True, alpha=0.3)
    for xi, d in zip(x, deltas):
        ax2.annotate(
            f"{d:+.1f}%",
            (xi, d),
            textcoords="offset points",
            xytext=(0, 4 if d >= 0 else -14),
            ha="center",
            fontsize=8,
        )

    fig.tight_layout()
    fig.savefig(OUT / "ttft-crossover-l4.png", dpi=150)
    plt.close(fig)


def fairness_frontier() -> None:
    """Phase 5b efficiency-vs-fairness Pareto frontier (the differentiator)."""
    rows = list(csv.DictReader((BENCH / "phase5b-frontier.csv").open()))

    # Hand-placed label offsets where sweep points sit nearly on top of each other.
    label_offsets = {"0.25": (10, 4), "0.5": (10, -12), "0.75": (-46, 4), "1": (10, -4)}

    fig, ax = plt.subplots(figsize=(7.5, 5.5))
    for row in rows:
        cfg = row["config"]
        eff = float(row["overall_hit_rate_pct"])
        fair = float(row["min_tenant_pct"])
        if cfg.startswith("gdsf-elastic"):
            w = cfg.split("w=")[1]
            ax.scatter(eff, fair, s=90, color="#1f77b4", zorder=3)
            ax.annotate(
                f"w={w}",
                (eff, fair),
                textcoords="offset points",
                xytext=label_offsets.get(w, (8, 6)),
                fontsize=9,
            )
        elif "LRU" in cfg:
            ax.scatter(eff, fair, s=90, color="#888888", marker="s", zorder=3)
            ax.annotate("LRU baseline", (eff, fair), textcoords="offset points", xytext=(8, -2), fontsize=9)
        else:
            ax.scatter(eff, fair, s=90, color="#d62728", marker="^", zorder=3)
            ax.annotate("static caps (5a)", (eff, fair), textcoords="offset points", xytext=(8, -2), fontsize=9)

    # Connect the elastic sweep so the frontier reads as one curve.
    elastic = sorted(
        (
            (float(r["overall_hit_rate_pct"]), float(r["min_tenant_pct"]))
            for r in rows
            if r["config"].startswith("gdsf-elastic")
        ),
    )
    ax.plot(*zip(*elastic), color="#1f77b4", alpha=0.35, linestyle="-", zorder=2)

    ax.set_xlabel("overall hit rate, % (efficiency)")
    ax.set_ylabel("min-tenant hit rate, % (fairness)")
    ax.set_title(
        "Efficiency vs fairness — GDSF-elastic fairness_weight sweep\n"
        "(w=0: efficiency corner; w≥0.25: fairness plateau)"
    )
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(OUT / "fairness-frontier.png", dpi=150)
    plt.close(fig)


def longcontext_a100() -> None:
    """RunPod A100 long-context curve: the honest negative result (no 32k crossover)."""
    pts7 = _load_points("phase45-longcontext-qwen7b.json")
    pts14 = _load_points("phase45-longcontext-qwen14b.json")

    fig, ax = plt.subplots(figsize=(7.5, 5))
    ax.plot(
        [p["tokens"] for p in pts7],
        [p["baseline_p50"] for p in pts7],
        label="7B baseline",
        color="#888888",
        marker="o",
        linestyle="--",
    )
    ax.plot(
        [p["tokens"] for p in pts7],
        [p["warm_p50"] for p in pts7],
        label="7B cache warm",
        color="#1f77b4",
        marker="o",
    )
    ax.plot(
        [p["tokens"] for p in pts14],
        [p["baseline_p50"] for p in pts14],
        label="14B baseline",
        color="#444444",
        marker="s",
        linestyle="--",
    )
    ax.plot(
        [p["tokens"] for p in pts14],
        [p["warm_p50"] for p in pts14],
        label="14B cache warm",
        color="#d62728",
        marker="s",
    )
    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_xlabel("shared-prefix length (tokens, log)")
    ax.set_ylabel("TTFT p50 (ms, log)")
    ax.set_title(
        "No crossover on A100 80GB — prefill is too fast, warm path is Python-bound\n"
        "(RunPod 1× A100; cache loses at every length up to 32k)"
    )
    ax.legend()
    ax.grid(True, alpha=0.3, which="both")
    fig.tight_layout()
    fig.savefig(OUT / "longcontext-a100.png", dpi=150)
    plt.close(fig)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    ttft_crossover_l4()
    fairness_frontier()
    longcontext_a100()
    for f in sorted(OUT.glob("*.png")):
        print(f"wrote {f.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
