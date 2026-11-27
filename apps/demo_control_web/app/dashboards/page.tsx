import {ArrowUpRight} from "lucide-react";
import {PageHeader} from "../../components/ui";
import {api} from "../../lib/api";
import {humanize} from "../../lib/tone";

export const dynamic = "force-dynamic";

type Dashboard = {uid: string; title: string; description: string; category: string; url: string};

export default async function Dashboards() {
  const data = await api<Dashboard[]>("/api/v1/dashboards");
  return (
    <div className="grid gap-5">
      <PageHeader title="Dashboards" description="Grafana dashboards provisioned with the stack. They open in a new tab." />
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
        {data.map((d) => (
          <a
            key={d.uid}
            href={d.url}
            target="_blank"
            rel="noreferrer"
            className="group grid gap-1.5 rounded-[var(--radius-panel)] border border-rule bg-panel p-5 transition-colors hover:border-rule-strong"
          >
            <span className="text-xs text-muted">{humanize(d.category)}</span>
            <span className="flex items-center justify-between gap-2 font-semibold text-ink">
              {d.title}
              <ArrowUpRight className="size-4 text-faint transition-colors group-hover:text-flow" aria-hidden="true" />
            </span>
            <span className="text-sm text-muted">{d.description}</span>
          </a>
        ))}
      </div>
    </div>
  );
}
