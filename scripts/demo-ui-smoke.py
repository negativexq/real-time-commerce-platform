#!/usr/bin/env python3
"""Bounded HTTP smoke for every Demo Control Web route."""

import sys
import urllib.request

for route in ("", "scenarios", "runs", "fraud", "dlq", "infrastructure", "dashboards"):
    with urllib.request.urlopen(
        f"http://127.0.0.1:3003/{route}", timeout=10
    ) as response:
        if response.status != 200:
            sys.exit(f"{route}: HTTP {response.status}")
print("Demo UI routes are reachable.")
