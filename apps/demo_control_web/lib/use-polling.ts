"use client";

import {useEffect, useRef} from "react";

/**
 * Runs `task` now and then every `intervalMs` while the page is visible.
 *
 * - A hidden tab stops asking: nobody is looking, so the API and Prometheus
 *   get no load from it. Coming back refreshes at once if the data is stale.
 * - A tick is skipped while the previous one is still in flight, so a slow
 *   API never piles up overlapping requests.
 */
export function usePolling(task: () => Promise<unknown> | void, intervalMs: number) {
  const taskRef = useRef(task);
  taskRef.current = task;

  useEffect(() => {
    let timer: number | undefined;
    let inFlight = false;
    let lastRun = 0;
    let stopped = false;

    async function tick() {
      if (inFlight || stopped) return;
      inFlight = true;
      lastRun = Date.now();
      try {
        await taskRef.current();
      } finally {
        inFlight = false;
      }
    }

    function start() {
      if (timer !== undefined) return;
      timer = window.setInterval(() => void tick(), intervalMs);
    }

    function stop() {
      window.clearInterval(timer);
      timer = undefined;
    }

    function onVisibility() {
      if (document.visibilityState === "hidden") {
        stop();
        return;
      }
      if (Date.now() - lastRun >= intervalMs) void tick();
      start();
    }

    if (document.visibilityState !== "hidden") {
      void tick();
      start();
    }
    document.addEventListener("visibilitychange", onVisibility);
    return () => {
      stopped = true;
      stop();
      document.removeEventListener("visibilitychange", onVisibility);
    };
  }, [intervalMs]);
}
