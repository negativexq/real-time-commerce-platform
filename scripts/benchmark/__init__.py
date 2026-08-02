"""Reproducible performance/reliability benchmark for the local demo stack.

Every module here talks to the already-running Docker Compose stack from the
host (not from inside a container), reusing the real production classes in
``services.event_processor`` / ``services.event_generator`` and the demo
control API rather than reimplementing pipeline behaviour.
"""
