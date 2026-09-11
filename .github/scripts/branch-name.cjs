// Branch naming policy shared by continuous integration and the ruleset.
//
// .github/rulesets/branch-names.json is the single source of truth. GitHub
// enforces it when a branch is pushed to this repository; this script repeats
// the same decision on pull requests, where branches live in forks that no
// repository ruleset can reach.

const { readFileSync } = require('node:fs');
const { join } = require('node:path');

const HEADS = 'refs/heads/';

const escape = literal => literal.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

// Translates a GitHub ref-name fnmatch pattern: `*` stays inside one path
// segment, `**` crosses segments.
const glob = pattern => new RegExp(`^${pattern
  .split('**')
  .map(part => part.split('*').map(escape).join('[^/]*'))
  .join('.*')}$`);

const ruleset = JSON.parse(
  readFileSync(join(__dirname, '..', 'rulesets', 'branch-names.json'), 'utf8')
);
const rule = ruleset.rules.find(entry => entry.type === 'branch_name_pattern');
if (!rule || rule.parameters.operator !== 'regex' || rule.parameters.negate) {
  throw new Error('The ruleset must hold one plain regex branch_name_pattern rule');
}

const pattern = new RegExp(rule.parameters.pattern);
const prefixes = /^\^\(([a-z|]+)\)\//.exec(rule.parameters.pattern)?.[1].split('|') ?? [];
if (prefixes.length < 1) {
  throw new Error('The rule pattern must begin with an alternation of approved prefixes');
}
const exempt = (ruleset.conditions.ref_name.exclude ?? [])
  .filter(entry => entry.startsWith(HEADS))
  .map(entry => glob(entry.slice(HEADS.length)));

// Returns null when the branch is acceptable, otherwise contributor guidance.
function check(branch) {
  if (typeof branch !== 'string' || branch.length === 0) {
    return 'No branch name was provided; expected the head branch of a pull request.';
  }
  if (exempt.some(exception => exception.test(branch)) || pattern.test(branch)) {
    return null;
  }
  const example = `${prefixes.includes('feature') ? 'feature' : prefixes[0]}/short-description`;
  return [
    `Branch "${branch}" does not follow the naming policy.`,
    `Approved prefixes: ${prefixes.map(prefix => `${prefix}/`).join(', ')}.`,
    'Anything after the prefix is free-form. Rename the branch and repoint the',
    'pull request, or open a new one from the renamed branch:',
    `  git branch -m ${example}`,
    `  git push origin -u ${example}`,
    `  git push origin --delete ${branch}`
  ].join('\n');
}

if (require.main === module) {
  const branch = process.argv[2] ?? process.env.GITHUB_HEAD_REF ?? '';
  const problem = check(branch);
  if (problem !== null) {
    console.error(problem);
    process.exitCode = 1;
  } else {
    console.log(`Branch "${branch}" follows the naming policy.`);
  }
}

module.exports = { check, pattern, prefixes };
