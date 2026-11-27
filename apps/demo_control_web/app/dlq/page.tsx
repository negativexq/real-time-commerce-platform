import {X} from "lucide-react";
import Link from "next/link";
import {CategoryBulkActions} from "../../components/dead-letter-bulk";
import {DeadLetterTable} from "../../components/dead-letter-table";
import {Meter} from "../../components/meter";
import {Badge, Empty, PageHeader, Panel, StatusDot} from "../../components/ui";
import {api} from "../../lib/api";
import {cn} from "../../lib/cn";
import {apiQuery, DLQ_STATUSES, filterHref, readFilters, STATUS_TONE, type DlqPage} from "../../lib/dead-letters";
import {formatCount} from "../../lib/formatting";
import {humanize} from "../../lib/tone";
import {Time} from "../../components/time";

export const dynamic = "force-dynamic";

const STATUS_HINT: Record<(typeof DLQ_STATUSES)[number], string> = {
  NEW: "Not looked at yet",
  TRIAGED: "Understood, kept for follow-up",
  REPLAYED: "Sent back to the pipeline",
  DISCARDED: "Not worth acting on",
};

export default async function Dlq({searchParams}: {searchParams: Promise<Record<string, string | string[] | undefined>>}) {
  const filters = readFilters(await searchParams);
  const data = await api<DlqPage>(apiQuery(filters));
  const filtered = Boolean(filters.status || filters.category);

  return (
    <div className="grid gap-5">
      <PageHeader
        title="Dead letters"
        description="Events the processor could not apply. The processor publishes them to Kafka; the DLQ sink copies each one here so it can be triaged and, where a second try can work, replayed."
      />
      <div className="grid gap-5 lg:grid-cols-[minmax(260px,1fr)_minmax(0,2fr)]">
        <Panel>
          <div className="text-[13px] text-muted">Waiting for a look</div>
          <div className={`figure mt-1 text-[56px] font-semibold leading-none ${data.statuses.NEW ? "text-warn" : "text-ok"}`}>
            {formatCount(data.statuses.NEW ?? 0)}
          </div>
          <p className="mt-2 text-sm text-muted">
            {data.total ? `of ${formatCount(data.total)} stored dead letters.` : "Nothing has failed. Every event was applied or recognised as a duplicate."}
          </p>
          {data.total > 0 && (
            <ul className="mt-4 grid gap-1.5 text-sm">
              {DLQ_STATUSES.map((status) => (
                <li key={status}>
                  <Link
                    href={filterHref({...filters, status: filters.status === status ? undefined : status})}
                    aria-current={filters.status === status ? "true" : undefined}
                    title={STATUS_HINT[status]}
                    className={cn(
                      "-mx-2 flex items-center gap-2 rounded-[var(--radius-control)] px-2 py-1 transition-colors hover:bg-raised",
                      filters.status === status && "bg-raised",
                    )}
                  >
                    <StatusDot tone={STATUS_TONE[status]} />
                    <span className="text-ink-soft">{humanize(status)}</span>
                    <span className="figure ml-auto text-ink">{formatCount(data.statuses[status] ?? 0)}</span>
                  </Link>
                </li>
              ))}
            </ul>
          )}
          <p className="mt-4 text-xs text-faint">
            {data.last_ingested_at ? (
              <>
                Last copied from Kafka <Time value={data.last_ingested_at} />
              </>
            ) : (
              "The DLQ sink has not stored anything yet."
            )}
          </p>
        </Panel>
        <Panel title="By failure category" description="Replayable categories can succeed on a second try. The others fail the same way every time.">
          {data.categories.length === 0 ? (
            <p className="text-sm text-muted">No categories to show.</p>
          ) : (
            <div className="grid gap-4">
              {data.categories.map((c) => (
                <div key={c.error_category} className="grid grid-cols-[minmax(0,1fr)_auto] items-end gap-2">
                  <Meter
                    label={
                      <Link href={filterHref({...filters, category: c.error_category})} className="inline-flex items-center gap-2 hover:text-ink hover:underline">
                        {humanize(c.error_category)}
                        {c.replayable && <Badge value="Replayable" tone="flow" />}
                      </Link>
                    }
                    value={c.count}
                    max={data.total}
                    tone="warn"
                    valueText={
                      <>
                        {formatCount(c.count)}
                        {c.new > 0 && c.new < c.count && <span className="text-muted">, {formatCount(c.new)} new</span>}
                      </>
                    }
                  />
                  {c.new > 0 ? <CategoryBulkActions category={c.error_category} newCount={c.new} /> : <span className="size-7" aria-hidden="true" />}
                </div>
              ))}
            </div>
          )}
        </Panel>
      </div>

      <Panel
        title={filtered ? "Matching dead letters" : "Most recent"}
        description={
          data.matching > data.items.length
            ? `Showing the latest ${data.items.length} of ${formatCount(data.matching)}. Open one to see its record and act on it.`
            : "Open one to see its record and act on it."
        }
        action={
          filtered ? (
            <div className="flex flex-wrap items-center gap-2">
              {filters.status && <FilterChip label={humanize(filters.status)} href={filterHref({...filters, status: undefined})} />}
              {filters.category && <FilterChip label={humanize(filters.category)} href={filterHref({...filters, category: undefined})} />}
              <Link href="/dlq" className="text-sm text-muted hover:text-ink">
                Clear filters
              </Link>
            </div>
          ) : null
        }
      >
        {!data.items.length ? (
          <Empty
            text={
              filtered
                ? "Nothing matches these filters."
                : "No dead letters. The malformed-event scenario sends invalid records here on purpose."
            }
          />
        ) : (
          <DeadLetterTable items={data.items} />
        )}
      </Panel>
    </div>
  );
}

function FilterChip({label, href}: {label: string; href: string}) {
  return (
    <Link
      href={href}
      aria-label={`Remove filter ${label}`}
      className="inline-flex h-7 items-center gap-1 rounded-full border border-rule bg-raised pr-1.5 pl-2.5 text-xs text-ink-soft hover:border-rule-strong hover:text-ink [&_svg]:size-3.5"
    >
      {label}
      <X aria-hidden="true" />
    </Link>
  );
}
