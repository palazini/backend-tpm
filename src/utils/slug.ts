export type SlugOptions = {
  trimWhitespace?: boolean;
  edgePattern?: RegExp;
};

const DEFAULT_EDGE_PATTERN = /^_+|_+$/g;

export function toSlug(input: string, options: SlugOptions = {}): string {
  const { trimWhitespace = true, edgePattern = DEFAULT_EDGE_PATTERN } = options;
  let value = String(input ?? "");

  if (trimWhitespace) {
    value = value.trim();
  }

  const slug = value
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_");

  return slug.replace(edgePattern, "");
}

export const slugify = (input: string): string => toSlug(input, { edgePattern: /^_|_$/g });

export const slugifyItem = (input: string): string => toSlug(input);
