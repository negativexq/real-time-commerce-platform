"use client";
import {useEffect,useState} from "react";
import {api,publicApiBase} from "../lib/api";
import type {Run} from "../lib/types";
import {Badge,Metric} from "./ui";
export function LiveRun({initial}: {initial: Run}) {const [run,setRun]=useState(initial); const [stale,setStale]=useState(false);
 useEffect(()=>{if(["COMPLETED","FAILED","STOPPED"].includes(run.status))return; const source=new EventSource(`${publicApiBase}/api/v1/runs/${run.run_id}/stream`); source.addEventListener("progress",e=>{setRun(JSON.parse((e as MessageEvent).data));setStale(false)}); source.onerror=()=>{setStale(true);source.close()}; return()=>source.close()},[run.run_id,run.status]);
 async function stop(){setRun(await api<Run>(`/api/v1/runs/${run.run_id}/stop`,{method:"POST"}))}
 return <><div className="run-head"><div><h2>{run.scenario_type.replaceAll("_"," ")}</h2><code>{run.run_id}</code></div><Badge value={run.status}/></div>{stale&&<p className="warning">Live stream interrupted. Refresh for current data.</p>}<div className="metrics"><Metric label="Generated" value={run.generated_event_count}/><Metric label="Processed" value={run.processed_event_count}/><Metric label="Approve" value={run.approve_count}/><Metric label="Review" value={run.review_count}/><Metric label="Block" value={run.block_count}/><Metric label="Alerts" value={run.fraud_alert_count}/><Metric label="DLQ" value={run.dlq_count}/></div><div className="progress"><span style={{width:`${Math.min(100,run.generated_event_count/run.requested_event_count*100)}%`}}/></div>{["RUNNING","STARTING"].includes(run.status)&&<button className="danger" onClick={stop}>Stop run</button>}</>
}
