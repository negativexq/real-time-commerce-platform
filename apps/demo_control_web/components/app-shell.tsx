import Link from "next/link";
import type {ReactNode} from "react";
import {apiDocsUrl} from "../lib/urls";

const links = [["/", "Overview"], ["/scenarios", "Scenarios"], ["/runs", "Runs"], ["/fraud", "Fraud"], ["/dlq", "DLQ"], ["/infrastructure", "Infrastructure"], ["/dashboards", "Dashboards"]];
export function AppShell({children}: {children: ReactNode}) {
  return <div className="shell"><aside><div className="brand"><span>RT</span><div>Commerce<small>Control Center</small></div></div><nav>{links.map(([href,label]) => <Link key={href} href={href}>{label}</Link>)}<a href={apiDocsUrl} target="_blank" rel="noreferrer">API Docs ↗</a></nav><div className="aside-foot">Local demo environment<br/><i>At-least-once pipeline</i></div></aside><main><header><div><small>REAL-TIME COMMERCE PLATFORM</small><h1>Operations Console</h1></div><span className="live">● LOCAL</span></header>{children}</main></div>
}
