import type {ReactNode} from "react";
import {apiDocsUrl} from "../lib/urls";
import {NavLinks} from "./nav-links";
import {NotificationBell} from "./notification-bell";
import {ThemeToggle} from "./theme-toggle";

/** Three stages joined by a line: the pipeline, as a mark. */
function BrandMark() {
  return (
    <svg viewBox="0 0 32 32" className="size-8 shrink-0" aria-hidden="true">
      <rect width="32" height="32" rx="8" style={{fill: "var(--color-raised)"}} />
      <line x1="8" y1="16" x2="24" y2="16" style={{stroke: "var(--color-rule-strong)"}} strokeWidth="2" />
      <circle cx="8" cy="16" r="3" style={{fill: "var(--color-flow)"}} />
      <circle cx="16" cy="16" r="3" style={{fill: "var(--color-flow)"}} opacity="0.7" />
      <circle cx="24" cy="16" r="3" style={{fill: "var(--color-flow)"}} opacity="0.4" />
    </svg>
  );
}

export function AppShell({children}: {children: ReactNode}) {
  return (
    <div className="min-h-screen lg:grid lg:grid-cols-[232px_minmax(0,1fr)]">
      <aside className="sticky top-0 z-10 border-b border-rule bg-panel/95 px-4 py-3 backdrop-blur lg:flex lg:h-screen lg:flex-col lg:border-r lg:border-b-0 lg:bg-panel lg:px-4 lg:py-6">
        <div className="mb-3 flex items-center gap-3 px-1 lg:mb-8">
          <BrandMark />
          <div className="leading-tight">
            <div className="font-semibold text-ink">Commerce</div>
            <div className="text-xs text-muted">Control Center</div>
          </div>
          <NotificationBell className="ml-auto" />
        </div>
        <NavLinks apiDocsUrl={apiDocsUrl} />
        <div className="mt-auto hidden lg:grid lg:gap-3">
          <ThemeToggle />
          <p className="px-3 text-xs leading-relaxed text-faint">
            Local Compose stack
            <br />
            At-least-once delivery
          </p>
        </div>
      </aside>
      <main className="mx-auto w-full max-w-[1480px] px-4 py-6 sm:px-6 lg:px-8 lg:py-8">{children}</main>
    </div>
  );
}
