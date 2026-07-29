"use client";
import {useState} from "react";
import {useRouter} from "next/navigation";
import {api} from "../lib/api";
import type {Run, Scenario} from "../lib/types";

export function ScenarioForm({scenarios}: {scenarios: Scenario[]}) {
 const router=useRouter(); const [selected,setSelected]=useState(scenarios[0]?.scenario_type ?? "normal_customer"); const [count,setCount]=useState(500); const [rate,setRate]=useState(20); const [seed,setSeed]=useState(42); const [error,setError]=useState("");
 const current=scenarios.find(s=>s.scenario_type===selected);
 async function start(){setError(""); try {const body:any={scenario_type:selected,event_count:count,events_per_second:rate,seed}; if(selected==="mixed_traffic") body.persona_distribution={normal:50,suspicious:20,bot:10,account_takeover:10,discount_hunter:5,indecisive:5}; const run=await api<Run>("/api/v1/runs",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)}); router.push(`/runs/${run.run_id}`)} catch(e){setError(e instanceof Error?e.message:"Unable to start")}}
 return <div className="scenario-form"><label>Scenario<select value={selected} onChange={e=>setSelected(e.target.value)}>{scenarios.map(s=><option key={s.scenario_type} value={s.scenario_type}>{s.title}</option>)}</select></label><div className="explain"><strong>{current?.purpose}</strong><span>Expected: {current?.expected_outcome}</span></div><div className="form-grid"><label>Event count<input type="number" min="1" max="100000" value={count} onChange={e=>setCount(Number(e.target.value))}/></label><label>Events / second<input type="number" min="1" max="1000" value={rate} onChange={e=>setRate(Number(e.target.value))}/></label><label>Deterministic seed<input type="number" value={seed} onChange={e=>setSeed(Number(e.target.value))}/></label></div><details><summary>Advanced settings</summary><p>Scenario-specific anomalies and identity behavior are fixed and allow-listed by the API.</p></details>{error&&<p className="error">{error}</p>}<button onClick={start} disabled={count<1||rate<1}>Start scenario</button></div>
}
