/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

// Temporary diagnostic: a lost runner may never finalize its current step log.
// Finalize small artifacts independently, using the workflow's pinned uploader.
const {spawn} = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const workspace = process.env.GITHUB_WORKSPACE;
const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'runner-resources-'));
const uploader = path.join(workspace, '.github/diagnostic-tools/upload-artifact/dist/upload/index.js');
let samples = '';
let sequence = 0;
let uploading;
let uploadChild;
let stopping = false;

const sampler = spawn('python3', ['-u', path.join(workspace, '.github/scripts/runner_resources.py')], {
  stdio: ['ignore', 'pipe', 'inherit'],
});
sampler.stdout.on('data', data => {
  process.stdout.write(data);
  // Bound both in-memory diagnostics and each artifact, even on a long step.
  samples = (samples + data.toString()).slice(-4 * 1024 * 1024);
});
sampler.on('error', error => console.error(`Resource sampler unavailable: ${error.code}`));

/** Upload one immutable checkpoint; unavailable diagnostics must not mask test status. */
async function checkpoint() {
  if (stopping || uploading || !samples) return;
  const number = ++sequence;
  const filename = path.join(directory, `sample-${number}.txt`);
  fs.writeFileSync(filename, samples);
  const name = `runner-resources-${process.env.GITHUB_RUN_ID}-${process.env.GITHUB_RUN_ATTEMPT}-${number}`;
  uploading = new Promise(resolve => {
    uploadChild = spawn(process.execPath, [uploader], {
      stdio: 'inherit',
      timeout: 20000,
      killSignal: 'SIGKILL',
      env: {
        ...process.env,
        INPUT_NAME: name,
        INPUT_PATH: filename,
        'INPUT_IF-NO-FILES-FOUND': 'error',
        'INPUT_RETENTION-DAYS': '3',
        'INPUT_COMPRESSION-LEVEL': '0',
        INPUT_OVERWRITE: 'false',
        'INPUT_INCLUDE-HIDDEN-FILES': 'false',
        INPUT_ARCHIVE: 'true',
      },
    });
    uploadChild.on('error', error => {
      console.error(`Resource upload ${number} unavailable: ${error.code}`);
      resolve();
    });
    uploadChild.on('exit', (code, signal) => {
      console.log(`Resource checkpoint ${number}: exit=${code}, signal=${signal}`);
      resolve();
    });
  });
  await uploading;
  uploading = undefined;
  uploadChild = undefined;
  fs.rmSync(filename, {force: true});
}

// Match the original run step's login shell and command. Its process group lets
// cancellation reach Cargo and linkers instead of leaving them detached.
const workload = spawn('bash', ['-l', '-eo', 'pipefail', '-c', 'make rust-tests'], {
  stdio: 'inherit',
  detached: true,
});
const completed = new Promise(resolve => {
  workload.on('error', error => {
    console.error(`Cannot start Rust tests: ${error.code}`);
    resolve(127);
  });
  workload.on('exit', (code, signal) => resolve(code ?? 128 + os.constants.signals[signal]));
});

for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, () => {
    stopping = true;
    if (workload.pid) {
      try { process.kill(-workload.pid, signal); } catch (error) {
        if (error.code !== 'ESRCH') console.error(`Cannot forward ${signal}: ${error.code}`);
      }
    }
    sampler.kill(signal);
    uploadChild?.kill('SIGKILL');
  });
}

// The first artifact must be visible early; later snapshots cover the failure window.
const first = setTimeout(() => checkpoint().catch(console.error), 5000);
const interval = setInterval(() => checkpoint().catch(console.error), 60000);
completed.then(async code => {
  process.exitCode = code;
  clearTimeout(first);
  clearInterval(interval);
  sampler.kill();
  if (uploading) await uploading;
  await checkpoint();
}).catch(error => {
  console.error(`Diagnostic cleanup failed: ${error.message}`);
});
