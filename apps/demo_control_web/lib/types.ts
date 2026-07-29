export type HealthState = "HEALTHY" | "DEGRADED" | "UNHEALTHY" | "UNKNOWN";
export type RunStatus = "PENDING" | "STARTING" | "RUNNING" | "STOP_REQUESTED" | "STOPPED" | "COMPLETED" | "FAILED";
export interface Scenario {scenario_type: string; title: string; purpose: string; expected_outcome: string; transaction_configurable: boolean}
export interface Run {run_id: string; scenario_type: string; status: RunStatus; requested_event_count: number; generated_event_count: number; processed_event_count: number; approve_count: number; review_count: number; block_count: number; fraud_alert_count: number; dlq_count: number; created_at: string; updated_at: string}
