"""Generates the README's diagrams as self-contained SVGs in the console's light
palette. Beams are animated with CSS; GitHub plays the animation when the SVG is
shown as an image, and the motion stops for readers who ask for reduced motion.

    python3 docs/diagrams/build_diagrams.py
"""

from __future__ import annotations

from pathlib import Path
from xml.sax.saxutils import escape

OUT = Path(__file__).resolve().parent

# The console's tokens (apps/demo_control_web/app/globals.css, light theme).
BASE, PANEL, INK, MUTED, RULE = "#f3f5f8", "#ffffff", "#151c25", "#5d6673", "#dfe4ea"
FLOW, OK, WARN, BAD, NEUTRAL = "#0e8a91", "#2f7d45", "#a86a0c", "#c23b28", "#7b8491"
SANS = (
    "ui-sans-serif, -apple-system, BlinkMacSystemFont, 'Segoe UI', Inter, "
    "Helvetica, Arial, sans-serif"
)
MONO = "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace"

STYLE = f"""
  .t {{ font-family: {SANS}; fill: {INK}; }}
  .m {{ font-family: {MONO}; fill: {INK}; }}
  .mut {{ fill: {MUTED}; }}
  .flow {{ stroke-dasharray: 6 8; animation: flow 1.6s linear infinite; }}
  .fast {{ animation-duration: 0.8s; }}
  .slow {{ animation-duration: 3.2s; }}
  @keyframes flow {{ to {{ stroke-dashoffset: -28; }} }}
  .pulse {{ animation: pulse 2.4s ease-in-out infinite; }}
  @keyframes pulse {{ 0%, 100% {{ opacity: 1; }} 50% {{ opacity: .35; }} }}
  @media (prefers-reduced-motion: reduce) {{ .flow, .pulse {{ animation: none; }} }}
"""


def svg(width: int, height: int, body: list[str], title: str) -> str:
    return "\n".join(
        [
            f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
            f'width="{width}" height="{height}" role="img" '
            f'aria-label="{escape(title)}">',
            f"<title>{escape(title)}</title>",
            f"<style>{STYLE}</style>",
            "<defs>",
            '  <filter id="sh" x="-10%" y="-10%" width="120%" height="140%">'
            '<feDropShadow dx="0" dy="1" stdDeviation="1.6" flood-color="#0f172a" '
            'flood-opacity=".07"/></filter>',
            *[
                f'  <marker id="ah-{name}" viewBox="0 0 10 10" refX="8" refY="5" '
                f'markerWidth="7" markerHeight="7" '
                f'orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" '
                f'fill="{color}"/></marker>'
                for name, color in (
                    ("flow", FLOW),
                    ("ok", OK),
                    ("warn", WARN),
                    ("bad", BAD),
                    ("mut", MUTED),
                )
            ],
            "</defs>",
            f'<rect x="0.5" y="0.5" width="{width - 1}" height="{height - 1}" rx="16" '
            f'fill="{BASE}" stroke="{RULE}"/>',
            *body,
            "</svg>",
            "",
        ]
    )


def text(
    x: float,
    y: float,
    s: str,
    *,
    size: float = 13,
    weight: int = 400,
    cls: str = "t",
    anchor: str = "start",
    fill: str | None = None,
) -> str:
    f = f' style="fill:{fill}"' if fill else ""
    return (
        f'<text x="{x}" y="{y}" class="{cls}" font-size="{size}" '
        f'font-weight="{weight}" '
        f'text-anchor="{anchor}"{f}>{escape(s)}</text>'
    )


def box(
    x: float,
    y: float,
    w: float,
    h: float,
    *,
    rx: float = 10,
    stroke: str = RULE,
    fill: str = PANEL,
    dash: bool = False,
    shadow: bool = True,
) -> str:
    d = ' stroke-dasharray="4 4"' if dash else ""
    s = ' filter="url(#sh)"' if shadow else ""
    return (
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" fill="{fill}" '
        f'stroke="{stroke}"{d}{s}/>'
    )


def dot(x: float, y: float, color: str, r: float = 4.5, pulse: bool = False) -> str:
    c = ' class="pulse"' if pulse else ""
    return f'<circle cx="{x}" cy="{y}" r="{r}" fill="{color}"{c}/>'


def node(
    x: float,
    y: float,
    w: float,
    h: float,
    title: str,
    sub: str = "",
    *,
    color: str | None = None,
    mono: bool = False,
    pulse: bool = False,
) -> list[str]:
    out = [box(x, y, w, h)]
    tx = x + 16
    if color:
        out.append(dot(x + 19, y + 24, color, pulse=pulse))
        tx = x + 31
    out.append(text(tx, y + 28.5, title, size=14, weight=600, cls="m" if mono else "t"))
    for i, line in enumerate(sub.split("\n") if sub else []):
        out.append(text(x + 16, y + 50 + i * 17, line, size=12, cls="t mut"))
    return out


def topic(x: float, y: float, w: float, name: str, sub: str, color: str) -> list[str]:
    return [
        f'<rect x="{x}" y="{y}" width="{w}" height="58" rx="9" fill="{BASE}" '
        f'stroke="{RULE}"/>',
        f'<rect x="{x}" y="{y + 10}" width="3" height="38" rx="1.5" fill="{color}"/>',
        text(x + 16, y + 25, name, size=13, weight=600, cls="m"),
        text(x + 16, y + 44, sub, size=11.5, cls="t mut"),
    ]


def beam(
    d: str,
    color: str,
    width: float,
    *,
    speed: str = "",
    arrow: str | None = None,
    dashed_track: bool = False,
) -> list[str]:
    td = ' stroke-dasharray="3 5"' if dashed_track else ""
    track = (
        f'<path d="{d}" fill="none" stroke="{RULE}" stroke-width="{width + 2}" '
        f'stroke-linecap="round"{td}/>'
    )
    m = f' marker-end="url(#ah-{arrow})"' if arrow else ""
    return [
        track,
        f'<path d="{d}" fill="none" stroke="{color}" stroke-width="{width}" '
        f'stroke-linecap="round" class="flow {speed}"{m}/>',
    ]


def label(x: float, y: float, s: str, color: str, anchor: str = "middle") -> str:
    return text(x, y, s, size=11.5, cls="m", anchor=anchor, fill=color)


def curve(x1: float, y1: float, x2: float, y2: float) -> str:
    mx = (x1 + x2) / 2
    return f"M{x1},{y1} C{mx},{y1} {mx},{y2} {x2},{y2}"


# --- architecture ---------------------------------------------------------------


def architecture() -> str:
    b: list[str] = []
    W, H = 1240, 690

    # Left column: what produces traffic and what people look at.
    b += node(
        40,
        70,
        230,
        96,
        "Commerce console",
        "Next.js web + FastAPI\nruns, fraud, dead-letter triage",
        color=FLOW,
    )
    b += node(
        40,
        250,
        230,
        96,
        "Scenario runner",
        "seeded customer journeys\nbounded count and rate",
        color=FLOW,
        pulse=True,
    )

    # Kafka with its three topics.
    b += [box(330, 40, 270, 436, rx=14)]
    b += [
        text(352, 72, "Kafka", size=16, weight=650),
        text(352, 92, "KRaft, single broker", size=12, cls="t mut"),
    ]
    b += topic(
        350, 118, 230, "commerce.events", "3 partitions, keyed by customer", FLOW
    )
    b += topic(
        350, 258, 230, "commerce.events.dlq", "invalid or exhausted records", WARN
    )
    b += topic(
        350, 398, 230, "commerce.fraud-alerts", "derived events from the outbox", BAD
    )

    # Middle: the workers.
    b += [box(660, 60, 260, 150)]
    b += [
        dot(679, 84, OK, pulse=True),
        text(691, 88.5, "Event processor", size=14, weight=600),
        text(904, 88.5, "Go", size=12, cls="m mut", anchor="end"),
    ]
    for i, step in enumerate(
        [
            "validate the contract",
            "take a Redis lease",
            "one PostgreSQL transaction",
            "commit the offset",
        ]
    ):
        y = 114 + i * 22
        b += [
            text(676, y, f"{i + 1}", size=11.5, weight=600, cls="m", fill=FLOW),
            text(694, y, step, size=12, cls="t mut"),
        ]
    b += node(
        660,
        262,
        260,
        84,
        "DLQ sink",
        "copies each dead letter once\ninto PostgreSQL",
        color=WARN,
    )
    b += node(
        660,
        392,
        260,
        84,
        "Fraud outbox publisher",
        "claims committed rows,\npublishes at least once",
        color=BAD,
    )

    # Right: state.
    b += node(980, 60, 220, 80, "Redis", "leases, completion markers", color=NEUTRAL)
    b += [box(980, 180, 220, 296, rx=12)]
    b += [
        dot(999, 204, OK),
        text(1011, 208.5, "PostgreSQL", size=14, weight=600),
        f'<line x1="997" y1="226" x2="1017" y2="226" stroke="{OK}" '
        f'stroke-dasharray="3 4"/>',
        text(1024, 230, "one transaction per event", size=12, cls="t", fill=OK),
    ]
    rows = [
        ("orders, payments, …", OK),
        ("fraud_evaluations", OK),
        ("processed_events", OK),
        ("fraud_outbox", BAD),
        ("dead_letters", WARN),
    ]
    row_y = {}
    for i, (name, color) in enumerate(rows):
        y = 252 + i * 44
        row_y[name] = y + 16
        b += [
            f'<rect x="996" y="{y}" width="188" height="32" rx="7" fill="{BASE}" '
            f'stroke="{RULE}"/>',
            dot(1010, y + 16, color, 3.5),
            text(1021, y + 20.5, name, size=12, cls="m"),
        ]
    b += [
        f'<rect x="990" y="246" width="200" height="176" rx="9" fill="none" '
        f'stroke="{OK}" stroke-dasharray="3 4"/>'
    ]

    # Observability strip.
    b += [box(40, 540, 1160, 110, rx=14, shadow=False, fill=PANEL)]
    b += [
        text(62, 572, "Observability", size=14, weight=650),
        text(
            62,
            594,
            "Every service and exporter exposes /metrics,",
            size=12,
            cls="t mut",
        ),
        text(
            62,
            611,
            "with bounded labels: no run or business IDs.",
            size=12,
            cls="t mut",
        ),
    ]
    b += node(
        400, 558, 200, 74, "Prometheus", "scrapes every 10 s, alert rules", color=WARN
    )
    b += node(720, 558, 200, 74, "Grafana", "7 provisioned dashboards", color=FLOW)
    b += beam("M600,595 L720,595", FLOW, 2, arrow="flow")
    b += [
        text(980, 584, "Alerts reach the console's", size=12, cls="t mut"),
        text(980, 601, "notification bell; critical", size=12, cls="t mut"),
        text(980, 618, "ones stay until resolved.", size=12, cls="t mut"),
    ]

    # Edges.
    b += beam(curve(270, 298, 350, 160), FLOW, 3, speed="fast", arrow="flow")
    b.append(label(280, 322, "events", FLOW, anchor="start"))
    b += beam(
        curve(270, 118, 350, 140),
        WARN,
        1.6,
        speed="slow",
        arrow="warn",
        dashed_track=True,
    )
    b.append(label(306, 108, "replay", WARN))
    b += beam(curve(580, 147, 660, 135), FLOW, 3, speed="fast", arrow="flow")
    b += beam("M920,100 L980,100", NEUTRAL, 1.6, arrow="mut")
    b += beam(curve(920, 170, 990, 268), OK, 3, arrow="ok")
    b += beam(curve(660, 190, 580, 280), WARN, 2, speed="slow", arrow="warn")
    b.append(label(632, 264, "DLQ", WARN, anchor="start"))
    b += beam(curve(580, 300, 660, 304), WARN, 2, speed="slow", arrow="warn")
    b += beam(
        curve(920, 304, 996, row_y["dead_letters"]), WARN, 2, speed="slow", arrow="warn"
    )
    b += beam(curve(996, row_y["fraud_outbox"], 920, 434), BAD, 2, arrow="bad")
    b += beam(curve(660, 434, 580, 427), BAD, 2, arrow="bad")

    return svg(
        W,
        H,
        b,
        "Architecture: the scenario runner publishes to Kafka; the Go processor takes "
        "a Redis lease "
        "and writes each event in one PostgreSQL transaction; failures go to a DLQ "
        "topic and a sink "
        "copies them into PostgreSQL for triage and replay; an outbox publisher sends "
        "fraud alerts",
    )


# --- dead-letter loop -------------------------------------------------------------


def dlq_loop() -> str:
    b: list[str] = []
    W, H = 1240, 440
    y = 150

    b += node(
        40,
        y,
        200,
        96,
        "Event processor",
        "validation failure,\npermanent error or\nretries exhausted",
        color=OK,
    )
    b += topic(300, y + 19, 200, "commerce.events.dlq", "full record + error", WARN)
    b += node(
        560, y, 180, 96, "DLQ sink", "idempotent on the\nDLQ coordinates", color=WARN
    )
    b += [box(800, y - 30, 400, 160, rx=12)]
    b += [
        dot(819, y - 6, WARN),
        text(831, y - 1.5, "dead_letters", size=14, weight=600, cls="m"),
        text(
            1184, y - 1.5, "console: Dead letters", size=12, cls="t mut", anchor="end"
        ),
    ]
    states = [
        ("NEW", WARN, "not looked at yet"),
        ("TRIAGED", FLOW, "understood, kept"),
        ("DISCARDED", NEUTRAL, "not worth acting on"),
        ("REPLAYED", OK, "sent back to Kafka"),
    ]
    for i, (s, color, hint) in enumerate(states):
        cx = 816 + (i % 2) * 192
        cy = y + 22 + (i // 2) * 48
        b += [
            f'<rect x="{cx}" y="{cy}" width="176" height="38" rx="8" fill="{BASE}" '
            f'stroke="{RULE}"/>',
            dot(cx + 14, cy + 19, color, 4),
            text(cx + 26, cy + 17, s, size=12, weight=600, cls="m"),
            text(cx + 26, cy + 31, hint, size=10.5, cls="t mut"),
        ]

    b += beam("M240,198 L300,198", WARN, 2.5, speed="slow", arrow="warn")
    b += beam("M500,198 L560,198", WARN, 2.5, speed="slow", arrow="warn")
    b += beam("M740,198 L800,198", WARN, 2.5, speed="slow", arrow="warn")

    # Replay back to the source topic, over the top.
    b += beam("M1000,120 C1000,40 140,40 140,150", OK, 2, arrow="ok")
    b += [
        box(470, 22, 300, 38, rx=19, shadow=False),
        text(
            620,
            46,
            "Replay to commerce.events",
            size=12.5,
            weight=600,
            anchor="middle",
            fill=OK,
        ),
    ]

    # The gate.
    b += [box(40, 320, 1160, 92, rx=12, shadow=False)]
    b += [
        text(
            62,
            349,
            "Replay is offered only when a second try can work:",
            size=12.5,
            weight=600,
        ),
        text(
            62,
            370,
            "retry_exhausted or missing_business_dependency  ·  full, untruncated "
            "payload  ·  "
            "source topic on the allow-list  ·  status not already REPLAYED",
            size=12,
            cls="m mut",
        ),
    ]
    b += [
        text(
            62,
            393,
            "The row is locked while the record is sent; if Kafka does not confirm "
            "delivery, nothing changes.",
            size=12,
            cls="t mut",
        )
    ]
    b.append(
        text(
            40,
            290,
            "A replay that fails again lands as a new record, linked from the "
            "original.",
            size=12,
            cls="t mut",
        )
    )
    return svg(
        W,
        H,
        b,
        "Dead-letter loop: the processor publishes to commerce.events.dlq, the sink "
        "copies records "
        "into dead_letters, operators triage them, and replayable ones go back to "
        "commerce.events",
    )


if __name__ == "__main__":
    for name, build in (("architecture", architecture), ("dead-letter-loop", dlq_loop)):
        (OUT / f"{name}.svg").write_text(build(), encoding="utf-8")
        print(f"wrote {name}.svg")
