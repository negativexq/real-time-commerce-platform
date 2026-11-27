"use client";

import {useSyncExternalStore} from "react";
import {formatDateTime} from "../lib/formatting";

const subscribe = () => () => {};

/**
 * A timestamp in the viewer's timezone. The server renders in its own zone (UTC in the container),
 * so the text is swapped for the browser's local time once the page hydrates.
 */
export function Time({value}: {value: string | null | undefined}) {
  const hydrated = useSyncExternalStore(
    subscribe,
    () => true,
    () => false,
  );
  if (!value) return <>—</>;
  return (
    <time dateTime={value} suppressHydrationWarning>
      {hydrated ? formatDateTime(value) : formatDateTime(value, "UTC")}
    </time>
  );
}
