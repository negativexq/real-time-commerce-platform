import {api} from "../../../lib/api";import type {Run} from "../../../lib/types";import {LiveRun} from "../../../components/live-run";
export default async function RunPage({params}:{params:Promise<{runId:string}>}){const {runId}=await params;const run=await api<Run>(`/api/v1/runs/${runId}`);return <section className="card wide"><LiveRun initial={run}/></section>}
