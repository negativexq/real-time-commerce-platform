export function shortId(value: string): string {
  return value.slice(0, 8);
}

export function formatCount(value: number): string {
  return new Intl.NumberFormat("en-US").format(value);
}
