const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const { join } = require('node:path');
const { test } = require('node:test');

const { check, prefixes } = require('../scripts/branch-name.cjs');
const ruleset = JSON.parse(
  readFileSync(join(__dirname, '../rulesets/branch-names.json'), 'utf8')
);

const APPROVED = [
  'build', 'chore', 'ci', 'deps', 'docs', 'feature',
  'fix', 'perf', 'refactor', 'release', 'security', 'test'
];

test('the approved prefixes are the recorded set', () => {
  assert.deepEqual(prefixes, APPROVED);
});

test('every approved prefix accepts a branch', () => {
  for (const prefix of APPROVED) {
    assert.equal(check(`${prefix}/wal-group-commit`), null, prefix);
  }
});

test('only the prefix is constrained', () => {
  for (const branch of [
    'feature/WAL_GroupCommit',
    'feature/42-storage/pager',
    'fix/a',
    `chore/${'x'.repeat(200)}`
  ]) {
    assert.equal(check(branch), null, branch);
  }
});

test('unknown or missing prefixes are rejected', () => {
  for (const branch of ['bug/panic', 'wip', 'Feature/caps', 'feature', 'feature/', '/fix/x', '']) {
    assert.notEqual(check(branch), null, branch);
  }
});

test('rejection lists the prefixes and the rename commands', () => {
  const problem = check('my-branch');
  assert.match(problem, /Approved prefixes: build\/, chore\/, .*test\//);
  assert.match(problem, /git branch -m feature\/short-description/);
  assert.match(problem, /git push origin --delete my-branch/);
});

test('Dependabot branches stay exempt', () => {
  assert.equal(check('dependabot/cargo/serde-1.0.0'), null);
  assert.equal(check('dependabot/github_actions/actions/checkout-7'), null);
  assert.notEqual(check('dependabot'), null);
  assert.notEqual(check('mydependabot/cargo/x'), null);
});

test('the ruleset covers all branches, is active, and grants no bypass', () => {
  assert.equal(ruleset.target, 'branch');
  assert.equal(ruleset.enforcement, 'active');
  assert.deepEqual(ruleset.bypass_actors, []);
  assert.deepEqual(ruleset.conditions.ref_name.include, ['~ALL']);
  assert.ok(ruleset.conditions.ref_name.exclude.includes('~DEFAULT_BRANCH'));
  assert.ok(ruleset.conditions.ref_name.exclude.includes('refs/heads/dependabot/**'));
});

test('continuous integration runs the check on pull requests', () => {
  const workflow = readFileSync(join(__dirname, '../workflows/ci.yml'), 'utf8');
  assert.match(workflow, /node \.github\/scripts\/branch-name\.cjs/);
  const main = JSON.parse(readFileSync(join(__dirname, '../rulesets/main.json'), 'utf8'));
  const required = main.rules
    .find(rule => rule.type === 'required_status_checks')
    .parameters.required_status_checks
    .map(status => status.context);
  assert.ok(required.includes('Branch name'));
});
