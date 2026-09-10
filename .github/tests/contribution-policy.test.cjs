const assert = require('node:assert/strict');
const { createHash } = require('node:crypto');
const { readFileSync } = require('node:fs');
const { join } = require('node:path');
const { test } = require('node:test');

const workflow = readFileSync(join(__dirname, '../workflows/contribution-policy.yml'), 'utf8');
const source = workflow.split('          script: |\n')[1]
  .split('\n').map(line => line.replace(/^ {12}/, '')).join('\n');
const runPolicy = new (Object.getPrototypeOf(async function () {}).constructor)(
  'require', 'github', 'context', 'core', source
);
const hash = value => createHash('sha256').update(value).digest('hex');

async function check(options = {}) {
  const registry = {
    cla_version: '1.0',
    contributors: [{ github_id: 1, role: 'project-owner', email_sha256: [hash('owner@example.com')] }]
  };
  if (options.contributor) registry.contributors.push(options.contributor);
  const head = 'a'.repeat(40);
  const commit = {
    sha: head,
    author: { id: options.authorId ?? 1 },
    commit: {
      author: { name: 'Owner', email: 'owner@example.com' },
      verification: { verified: options.verified ?? true },
      message: options.message ?? 'Initial change\n\nSigned-off-by: Owner <owner@example.com>'
    }
  };
  const statuses = [];
  const errors = [];
  let reads = 0;
  let registryRef;
  const github = {
    rest: {
      pulls: {
        get: async () => ({ data: {
          state: 'open', commits: options.count ?? 1,
          head: { sha: options.changedHead && reads++ > 0 ? 'b'.repeat(40) : head }
        } }),
        listCommits: () => {}
      },
      repos: {
        get: async () => ({ data: { default_branch: 'main', owner: { id: 1 } } }),
        getContent: async args => {
          registryRef = args.ref;
          return { data: { content: Buffer.from(JSON.stringify(registry)).toString('base64') } };
        },
        getCommit: async () => ({ data: commit }),
        createCommitStatus: async status => statuses.push(status)
      }
    },
    paginate: async () => [commit]
  };
  await runPolicy(require, github, {
    repo: { owner: 'owner', repo: 'example' }, runId: 123,
    payload: { pull_request: { number: 1 } }
  }, { setFailed: message => errors.push(message) });
  return { statuses, errors, registryRef };
}

test('accepts verified owner contribution using the trusted default-branch registry', async () => {
  const result = await check();
  assert.equal(result.statuses.at(-1).state, 'success');
  assert.equal(result.registryRef, 'main');
  assert.deepEqual(result.errors, []);
});

test('rejects unsigned commits', async () => {
  const result = await check({ verified: false });
  assert.equal(result.statuses.at(-1).state, 'failure');
  assert.match(result.errors[0], /signature/);
});

test('rejects authors without a CLA', async () => {
  assert.match((await check({ authorId: 2 })).errors[0], /approved CLA/);
});

test('an arbitrary project-owner registry role does not bypass a CLA', async () => {
  assert.match((await check({
    authorId: 2, contributor: { github_id: 2, role: 'project-owner' }
  })).errors[0], /approved CLA/);
});

test('accepts a reviewed contributor with versioned acceptance evidence', async () => {
  const result = await check({ authorId: 2, contributor: {
    github_id: 2, cla_version: '1.0', accepted_at: '2026-09-10',
    evidence_sha256: 'f'.repeat(64), email_sha256: []
  } });
  assert.equal(result.statuses.at(-1).state, 'success');
});

test('requires a matching DCO trailer, not an arbitrary body mention', async () => {
  for (const message of [
    'Change without sign-off',
    'Change\n\nSigned-off-by: Another <another@example.com>',
    'Signed-off-by: Owner <owner@example.com>\n\nUnrelated final paragraph'
  ]) {
    assert.match((await check({ message })).errors[0], /DCO/);
  }
});

test('rejects unapproved coauthors even when the primary author is approved', async () => {
  const result = await check({
    message: 'Change\n\nCo-authored-by: Other <other@example.com>\nSigned-off-by: Owner <owner@example.com>'
  });
  assert.match(result.errors[0], /coauthor needs/);
});

test('requires coauthor sign-off after CLA email approval', async () => {
  const options = {
    contributor: {
      github_id: 2, cla_version: '1.0', accepted_at: '2026-09-10',
      evidence_sha256: 'f'.repeat(64), email_sha256: [hash('other@example.com')]
    },
    message: 'Change\n\nCo-authored-by: Other <other@example.com>\nSigned-off-by: Owner <owner@example.com>'
  };
  assert.match((await check(options)).errors[0], /coauthor DCO/);
  options.message += '\nSigned-off-by: Other <other@example.com>';
  assert.equal((await check(options)).statuses.at(-1).state, 'success');
});

test('fails closed on truncated commit lists or a changed PR head', async () => {
  for (const options of [{ count: 251 }, { count: 2 }, { changedHead: true }]) {
    assert.equal((await check(options)).statuses.at(-1).state, 'failure');
  }
});
