import Link from "next/link";
import {LiveRun} from "../../../components/live-run";
import {api} from "../../../lib/api";
import type {Run} from "../../../lib/types";

export const dynamic = "force-dynamic";

export default async function RunPage({params}: {params: Promise<{runId: string}>}) {
  const {runId} = await params;
  const run = await api<Run>(`/api/v1/runs/${runId}`);
  return (
    <div className="grid gap-4">
      <Link href="/runs" className="text-sm text-muted hover:text-ink">
        Back to runs
      </Link>
      <LiveRun initial={run} />
    </div>
  );
}
