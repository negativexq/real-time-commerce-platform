"use client";

import {
  Activity,
  BookOpen,
  Gauge,
  History,
  Inbox,
  LayoutDashboard,
  Play,
  ShieldAlert,
  type LucideIcon,
} from "lucide-react";
import Link from "next/link";
import {usePathname} from "next/navigation";
import {cn} from "../lib/cn";

const LINKS: [href: string, label: string, icon: LucideIcon][] = [
  ["/", "Overview", Gauge],
  ["/scenarios", "Scenarios", Play],
  ["/runs", "Runs", History],
  ["/fraud", "Fraud", ShieldAlert],
  ["/dlq", "Dead letters", Inbox],
  ["/infrastructure", "Infrastructure", Activity],
  ["/dashboards", "Dashboards", LayoutDashboard],
];

const item =
  "group flex shrink-0 items-center gap-3 rounded-[var(--radius-control)] px-3 py-2 text-sm font-medium transition-colors [&_svg]:size-4 [&_svg]:shrink-0";

export function NavLinks({apiDocsUrl}: {apiDocsUrl: string}) {
  const pathname = usePathname() ?? "";
  return (
    <nav aria-label="Main" className="flex gap-1 overflow-x-auto sm:flex-wrap sm:overflow-visible lg:flex-col lg:flex-nowrap">
      {LINKS.map(([href, label, Icon]) => {
        const active = href === "/" ? pathname === "/" : pathname.startsWith(href);
        return (
          <Link
            key={href}
            href={href}
            aria-current={active ? "page" : undefined}
            className={cn(
              item,
              active
                ? "bg-raised text-ink [&_svg]:text-flow"
                : "text-muted hover:bg-raised/60 hover:text-ink",
            )}
          >
            <Icon aria-hidden="true" />
            {label}
          </Link>
        );
      })}
      <a href={apiDocsUrl} target="_blank" rel="noreferrer" className={cn(item, "text-muted hover:bg-raised/60 hover:text-ink")}>
        <BookOpen aria-hidden="true" />
        API Docs ↗
      </a>
    </nav>
  );
}
