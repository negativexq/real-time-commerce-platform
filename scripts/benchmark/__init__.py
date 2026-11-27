"""Reproducible performance/reliability benchmark for the local demo stack.

Every module here talks to the already-running Docker Compose stack from the
host (not from inside a container), driving the running Go event processor
through the demo control API and reusing ``services.event_generator`` rather
than reimplementing pipeline behaviour.
"""
