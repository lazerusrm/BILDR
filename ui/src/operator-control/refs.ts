/** Crockford base32 ULID, as used for every run, task, and agent identifier. */
const ULID = /\b[0-7][0-9A-HJKMNP-TV-Z]{25}\b/g;

export type RefLabels = Record<string, string>;

/**
 * Replace raw identifiers with the names an operator recognises. An unknown id
 * collapses to its last five characters so a sentence stays readable without
 * inventing a name for something we cannot resolve.
 */
export function humanizeRefs(text: string, labels: RefLabels): string {
  return text.replace(ULID, (id) => labels[id] || `…${id.slice(-5)}`);
}

/**
 * Same resolution for a single identifier rendered on its own. Only an opaque
 * ULID is abbreviated; an id that already reads as a name is left alone.
 */
export function refLabel(id: string, labels: RefLabels): string {
  if (labels[id]) return labels[id];
  return new RegExp(`^${ULID.source}$`).test(id) ? `…${id.slice(-5)}` : id;
}
