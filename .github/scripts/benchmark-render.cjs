// Validation and rendering for the storage benchmark comparison.
//
// The unprivileged benchmark job writes a report; the privileged comment
// workflow runs the copy of this file from the default branch. Every field of
// a report therefore arrives from an untrusted job and is checked here before
// any of it reaches a pull request comment.

const MARKER = '<!-- evedb-storage-benchmark -->';

// Percent difference that is reported as a change rather than as noise.
const THRESHOLD = 10;

const NAME = /^[a-z0-9_]{1,64}$/;
const SHA = /^[0-9a-f]{40}$/;

const finite = value => typeof value === 'number' && Number.isFinite(value);

function validate(report) {
  if (report === null || typeof report !== 'object' || Array.isArray(report)) {
    throw new Error('The report must be an object');
  }
  const number = report.pull_request;
  if (!Number.isSafeInteger(number) || number < 1) throw new Error('Invalid pull request number');
  for (const key of ['base_sha', 'head_sha']) {
    if (!SHA.test(report[key] ?? '')) throw new Error(`Invalid ${key}`);
  }
  if (!Array.isArray(report.metrics) || report.metrics.length < 1 || report.metrics.length > 64) {
    throw new Error('The report must carry 1 to 64 measurements');
  }
  const metrics = report.metrics.map(metric => {
    if (metric === null || typeof metric !== 'object') throw new Error('Invalid measurement');
    if (!NAME.test(metric.name ?? '')) throw new Error('Invalid measurement name');
    if (!finite(metric.head) || metric.head < 0) throw new Error(`Invalid value for ${metric.name}`);
    if (metric.base !== null && metric.base !== undefined &&
        (!finite(metric.base) || metric.base < 0)) {
      throw new Error(`Invalid base value for ${metric.name}`);
    }
    return { name: metric.name, head: metric.head, base: finite(metric.base) ? metric.base : null };
  });
  const settings = {};
  for (const [key, value] of Object.entries(report.settings ?? {})) {
    if (!NAME.test(key) || !finite(value)) throw new Error('Invalid benchmark setting');
    settings[key] = value;
  }
  return {
    pull_request: number,
    base_sha: report.base_sha,
    head_sha: report.head_sha,
    settings,
    metrics,
    settings_match: report.settings_match === true
  };
}

const decimal = (value, digits) =>
  value.toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits });

function amount(name, value) {
  if (value === null) return '—';
  if (name.endsWith('_ms')) return `${decimal(value, 3)} ms`;
  if (/(^|_)bytes(_|$)/.test(name)) return `${decimal(value, 0)} bytes`;
  return decimal(value, 3);
}

// Every measurement counts down: milliseconds and stored bytes are both better
// when smaller.
function change(metric) {
  if (metric.base === null) return { text: 'no base measurement', direction: 0 };
  if (metric.base === 0) return { text: '—', direction: 0 };
  const percent = ((metric.head - metric.base) / metric.base) * 100;
  const text = `${percent >= 0 ? '+' : '−'}${decimal(Math.abs(percent), 1)}%`;
  if (percent <= -THRESHOLD) return { text: `${text} faster ✅`, direction: -1 };
  if (percent >= THRESHOLD) return { text: `${text} slower ⚠️`, direction: 1 };
  return { text, direction: 0 };
}

function render(report) {
  const { metrics, settings } = report;
  const changes = metrics.map(change);
  const faster = changes.filter(entry => entry.direction < 0).length;
  const slower = changes.filter(entry => entry.direction > 0).length;
  const measured = metrics.filter(metric => metric.base !== null).length;

  const lines = [`### Storage benchmark`, ''];
  if (!report.settings_match) {
    lines.push(
      'The base commit ran with different benchmark parameters, so its numbers',
      'are not comparable and this run reports the pull request alone.'
    );
  } else if (measured === 0) {
    lines.push(
      'The base commit produced no measurement, so this run reports the pull',
      'request alone. Nothing here says whether the change is faster or slower.'
    );
  } else if (slower === 0 && faster === 0) {
    lines.push(`No measurement moved by more than ${THRESHOLD}% against the base commit.`);
  } else {
    const parts = [];
    if (faster > 0) parts.push(`${faster} improved`);
    if (slower > 0) parts.push(`${slower} regressed`);
    lines.push(`Against the base commit, ${parts.join(' and ')} by more than ${THRESHOLD}%.`);
  }
  lines.push('');
  lines.push('| Measurement | Base | This branch | Change |');
  lines.push('| --- | ---: | ---: | ---: |');
  for (const [index, metric] of metrics.entries()) {
    lines.push(`| \`${metric.name}\` | ${amount(metric.name, metric.base)} | ` +
      `${amount(metric.name, metric.head)} | ${changes[index].text} |`);
  }
  lines.push('');
  const parameters = Object.entries(settings)
    .map(([key, value]) => `\`${key}=${decimal(value, 0)}\``).join(', ');
  lines.push(`Base \`${report.base_sha.slice(0, 12)}\` against head ` +
    `\`${report.head_sha.slice(0, 12)}\`${parameters ? `, ${parameters}` : ''}.`);
  lines.push('');
  lines.push(`One run per commit on one shared GitHub runner, with the operating-system ` +
    `file cache enabled and no percentiles. Timings vary between runs, so treat a mark ` +
    `as a reason to measure locally, not as a result. This check never blocks a merge.`);
  return lines.join('\n');
}

module.exports = { MARKER, THRESHOLD, validate, render };
