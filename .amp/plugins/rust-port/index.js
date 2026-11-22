// Amp reads `description` statically from the entry file, so it lives here, not in the bundle.
export const description =
  'Gatekeeper for staged C-to-Rust ports: verifies claimed revisions with checks and reviewers, tracks outstanding findings, and opens each stage PR on admission.';
export { default } from './dist/plugin.js';
