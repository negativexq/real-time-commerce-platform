import {ScenarioForm} from "../../components/scenario-form";
import {PageHeader} from "../../components/ui";
import {api} from "../../lib/api";
import type {Scenario} from "../../lib/types";

export const dynamic = "force-dynamic";

export default async function Scenarios() {
  const scenarios = await api<Scenario[]>("/api/v1/scenarios");
  return (
    <div className="grid gap-5">
      <PageHeader title="Scenarios" description="Send a reproducible burst of commerce traffic through the pipeline and watch how fraud rules respond." />
      <ScenarioForm scenarios={scenarios} />
    </div>
  );
}
