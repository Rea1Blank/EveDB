// Turns two storage_bench transcripts into one comparison report.
//
// Usage: node benchmark-report.cjs <base-transcript> <head-transcript> <report.json>
//
// The base transcript may be missing or empty; a base commit that predates the
// example, or a failed base run, still produces a report for this branch alone.

const { appendFileSync, mkdirSync, readFileSync, writeFileSync } = require('node:fs');
const { dirname } = require('node:path');

const { render, validate } = require('./benchmark-render.cjs');

// Parameters the example echoes back; they describe the run, not its cost.
const SETTINGS = ['rows', 'payload_bytes', 'batch_rows', 'checkpoint_bytes'];

const PAIR = /^([a-z0-9_]+)=(-?\d+(?:\.\d+)?)$/;

// The example prints one `key=value` per line, and comma-separated pairs on its
// parameter line. Prose lines carry no pair and are skipped.
function parse(path) {
  let text;
  try {
    text = readFileSync(path, 'utf8');
  } catch {
    return new Map();
  }
  const values = new Map();
  for (const line of text.split('\n')) {
    for (const pair of line.trim().split(',')) {
      const match = PAIR.exec(pair.trim());
      if (match !== null) values.set(match[1], Number(match[2]));
    }
  }
  return values;
}

const [basePath, headPath, outputPath] = process.argv.slice(2);
if (!basePath || !headPath || !outputPath) {
  console.error('Usage: benchmark-report.cjs <base-transcript> <head-transcript> <report.json>');
  process.exit(2);
}

const base = parse(basePath);
const head = parse(headPath);
if (head.size === 0) {
  console.error(`No measurement was parsed from ${headPath}`);
  process.exit(1);
}

const settings = {};
for (const key of SETTINGS) {
  if (head.has(key)) settings[key] = head.get(key);
}
const settingsMatch = base.size === 0 ||
  SETTINGS.every(key => base.get(key) === head.get(key));

const metrics = [...head.keys()]
  .filter(name => !SETTINGS.includes(name))
  .map(name => ({
    name,
    head: head.get(name),
    base: settingsMatch && base.has(name) ? base.get(name) : null
  }));

const report = validate({
  pull_request: Number(process.env.BENCH_PULL_REQUEST),
  base_sha: process.env.BENCH_BASE_SHA,
  head_sha: process.env.BENCH_HEAD_SHA,
  settings,
  settings_match: settingsMatch,
  metrics
});

mkdirSync(dirname(outputPath), { recursive: true });
writeFileSync(outputPath, `${JSON.stringify(report, null, 2)}\n`);

const markdown = render(report);
console.log(markdown);
if (process.env.GITHUB_STEP_SUMMARY) {
  appendFileSync(process.env.GITHUB_STEP_SUMMARY, `${markdown}\n`);
}
