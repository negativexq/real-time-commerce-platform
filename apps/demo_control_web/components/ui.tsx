import type {ReactNode} from "react";
export function Card({title, children}: {title: string; children: ReactNode}) {return <section className="card"><h2>{title}</h2>{children}</section>}
export function Badge({value}: {value: string}) {return <span className={`badge ${value.toLowerCase()}`}>{value.replaceAll("_", " ")}</span>}
export function Empty({text}: {text: string}) {return <div className="empty">{text}</div>}
export function Metric({label, value, unit}: {label: string; value: string|number|null|undefined; unit?: string}) {return <div className="metric"><small>{label}</small><strong>{value ?? "—"}{unit}</strong></div>}
