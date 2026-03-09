import { readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const workspaceRoot = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const version = parseReleaseVersion(process.argv[2]);

const packageJsonPaths = [
  'package.json',
  'apps/cli/package.json',
  'apps/web/package.json',
  'packages/api-contract/package.json',
  'packages/api-core/package.json',
];

for (const relativePath of packageJsonPaths) {
  const absolutePath = path.join(workspaceRoot, relativePath);
  const packageJson = JSON.parse(readFileSync(absolutePath, 'utf8'));
  packageJson.version = version;
  writeFileSync(absolutePath, `${JSON.stringify(packageJson, null, 2)}\n`);
  process.stdout.write(`updated ${relativePath} -> ${version}\n`);
}

function parseReleaseVersion(tagName) {
  if (!tagName) {
    throw new Error('Missing release tag. Expected vX.Y.Z');
  }
  const match = tagName.trim().match(/^v(\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?)$/);
  if (!match) {
    throw new Error(`Invalid release tag: ${tagName}. Expected vX.Y.Z`);
  }
  return match[1];
}
