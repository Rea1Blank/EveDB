const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');
const { mkdtempSync, readFileSync, writeFileSync } = require('node:fs');
const { tmpdir } = require('node:os');
const { join } = require('node:path');
const { test } = require('node:test');

const { MARKER, render, validate } = require('../scripts/benchmark-render.cjs');

const BASE_SHA = 'a'.repeat(40);
const HEAD_SHA = 'b'.repeat(40);

const transcript = (insert, scan, bytes) => [
  'rows=10000, payload_bytes=1024, batch_rows=128, checkpoint_bytes=8388608',
  `insert_ms=${insert}`,
  `scan_ms=${scan}`,
  `directory_bytes_after_retention=${bytes}`,
  'OS cache is enabled; no hard memory limit or power-loss simulation was applied.',
  ''
].join('\n');

function report(baseText, headText, env = {}) {
  const directory = mkdtempSync(join(tmpdir(), 'evedb-bench-'));
  const basePath = join(directory, 'base.txt');
  const headPath = join(directory, 'head.txt');
  const outputPath = join(directory, 'out', 'report.json');
  if (baseText !== null) writeFileSync(basePath, baseText);
  writeFileSync(headPath, headText);
  const markdown = execFileSync(
    process.execPath,
    [join(__dirname, '../scripts/benchmark-report.cjs'), basePath, headPath, outputPath],
    {
      encoding: 'utf8',
      env: {
        ...process.env,
        GITHUB_STEP_SUMMARY: '',
        BENCH_PULL_REQUEST: '12',
        BENCH_BASE_SHA: BASE_SHA,
        BENCH_HEAD_SHA: HEAD_SHA,
        ...env
      }
    }
  );
  return { markdown, json: JSON.parse(readFileSync(outputPath, 'utf8')) };
}

test('a comparison keeps settings apart from measurements', () => {
  const { json } = report(transcript(1000, 200, 100), transcript(900, 100, 90));
  assert.deepEqual(json.settings, {
    rows: 10000, payload_bytes: 1024, batch_rows: 128, checkpoint_bytes: 8388608
  });
  assert.equal(json.settings_match, true);
  assert.deepEqual(json.metrics.map(metric => metric.name),
    ['insert_ms', 'scan_ms', 'directory_bytes_after_retention']);
  assert.deepEqual(json.metrics[1], { name: 'scan_ms', head: 100, base: 200 });
});

test('improvements and regressions beyond the threshold are marked', () => {
  const { markdown } = report(transcript(1000, 200, 100), transcript(1500, 100, 100));
  assert.match(markdown, /`insert_ms`.*\+50\.0% slower ⚠️/);
  assert.match(markdown, /`scan_ms`.*−50\.0% faster ✅/);
  assert.match(markdown, /`directory_bytes_after_retention`.*\| \+0\.0% \|/);
  assert.match(markdown, /1 improved and 1 regressed by more than 10%/);
});

test('movement inside the threshold is reported without a mark', () => {
  const { markdown } = report(transcript(1000, 200, 100), transcript(1050, 190, 100));
  assert.match(markdown, /No measurement moved by more than 10%/);
  assert.doesNotMatch(markdown, /⚠️|✅/);
});

test('units follow the measurement name', () => {
  const { markdown } = report(transcript(1000, 200, 47565864), transcript(1000, 200, 47565864));
  assert.match(markdown, /1,000\.000 ms/);
  assert.match(markdown, /47,565,864 bytes/);
});

test('a missing base run still reports this branch', () => {
  const { json, markdown } = report(null, transcript(900, 100, 90));
  assert.deepEqual(json.metrics.map(metric => metric.base), [null, null, null]);
  assert.match(markdown, /The base commit produced no measurement/);
  assert.match(markdown, /no base measurement/);
});

test('mismatched settings drop the base column and say why', () => {
  const changed = transcript(1000, 200, 100).replace('rows=10000', 'rows=5000');
  const { json, markdown } = report(changed, transcript(900, 100, 90));
  assert.equal(json.settings_match, false);
  assert.deepEqual(json.metrics.map(metric => metric.base), [null, null, null]);
  assert.match(markdown, /different benchmark parameters/);
  assert.doesNotMatch(markdown, /produced no measurement/);
});

test('an unmeasurable branch run fails instead of reporting nothing', () => {
  assert.throws(() => report(transcript(1000, 200, 100), 'the example did not run\n'));
});

test('the comment carries the marker and its caveats', () => {
  const parsed = validate({
    pull_request: 12,
    base_sha: BASE_SHA,
    head_sha: HEAD_SHA,
    settings: { rows: 10000 },
    settings_match: true,
    metrics: [{ name: 'insert_ms', base: 1000, head: 900 }]
  });
  const body = `${MARKER}\n${render(parsed)}`;
  assert.ok(body.startsWith(MARKER));
  assert.match(body, /One run per commit on one shared GitHub runner/);
  assert.match(body, /never blocks a merge/);
  assert.match(body, new RegExp(`${BASE_SHA.slice(0, 12)}.*${HEAD_SHA.slice(0, 12)}`));
});

test('reports from an untrusted job are rejected before they are rendered', () => {
  const valid = {
    pull_request: 12,
    base_sha: BASE_SHA,
    head_sha: HEAD_SHA,
    settings: { rows: 10000 },
    settings_match: true,
    metrics: [{ name: 'insert_ms', base: 1000, head: 900 }]
  };
  const reject = (changes, description) =>
    assert.throws(() => validate({ ...valid, ...changes }), undefined, description);
  reject({ pull_request: 0 }, 'pull request number');
  reject({ pull_request: '12' }, 'pull request number type');
  reject({ head_sha: 'not-a-sha' }, 'head sha');
  reject({ metrics: [] }, 'empty measurements');
  reject({ metrics: [{ name: 'ok_ms', head: 'x' }] }, 'value type');
  reject({ metrics: [{ name: 'ok_ms', head: Number.NaN }] }, 'value range');
  reject({ metrics: [{ name: '`injected` | --- |', head: 1 }] }, 'measurement name');
  reject({ settings: { 'bad name': 1 } }, 'setting name');
  reject({ settings: { rows: 'many' } }, 'setting value');
  // A base value that is absent is allowed; a malformed one is not.
  assert.equal(validate({ ...valid, metrics: [{ name: 'ok_ms', head: 1 }] }).metrics[0].base, null);
  reject({ metrics: [{ name: 'ok_ms', head: 1, base: -5 }] }, 'negative base');
});

test('the comment workflow never checks out the measured code', () => {
  const workflow = readFileSync(join(__dirname, '../workflows/benchmark-comment.yml'), 'utf8');
  assert.match(workflow, /ref: \$\{\{ github\.event\.repository\.default_branch \}\}/);
  assert.match(workflow, /pull-requests: write/);
  assert.doesNotMatch(workflow, /pull_request_target/);
  const benchmark = readFileSync(join(__dirname, '../workflows/benchmark.yml'), 'utf8');
  assert.match(benchmark, /permissions:\n {2}contents: read\n/);
  assert.doesNotMatch(benchmark, /write/);
});
