import crypto from 'node:crypto';

const WORDS = [
  'anchor', 'apex', 'atlas', 'beacon', 'binary', 'bridge', 'cable', 'canvas',
  'cipher', 'clear', 'cloud', 'cobalt', 'comet', 'copper', 'delta', 'drift',
  'ember', 'engine', 'falcon', 'fiber', 'field', 'filter', 'focus', 'forge',
  'frame', 'garden', 'glide', 'harbor', 'helix', 'hollow', 'index', 'island',
  'kernel', 'keystone', 'lantern', 'lattice', 'ledger', 'level', 'maple', 'matrix',
  'meadow', 'merge', 'mirror', 'module', 'needle', 'noble', 'nova', 'orbit',
  'origin', 'parcel', 'patch', 'pillar', 'pixel', 'plume', 'portal', 'pulse',
  'quartz', 'quiet', 'radar', 'raven', 'relay', 'render', 'ripple', 'river',
  'signal', 'silver', 'sketch', 'socket', 'solar', 'span', 'spiral', 'spring',
  'stable', 'stone', 'summit', 'switch', 'thread', 'timber', 'token', 'trace',
  'union', 'vector', 'velvet', 'vertex', 'vessel', 'violet', 'vista', 'wave',
  'willow', 'window', 'yellow', 'zenith',
] as const;

export type HumanKey = {
  hash: string;
  slug: string;
  checksum: string;
};

export function stableHash(value: string): string {
  return crypto.createHash('sha256').update(value).digest('hex');
}

export function humanKeyFromHash(hash: string): HumanKey {
  const normalized = hash.toLowerCase();
  if (!/^[a-f0-9]{64}$/.test(normalized)) {
    throw new Error('Human key hash must be a SHA-256 hex digest');
  }

  const indexes = [0, 2, 4].map((offset) => Number.parseInt(normalized.slice(offset, offset + 2), 16) % WORDS.length);
  const checksum = Number.parseInt(normalized.slice(6, 12), 16).toString(36).padStart(4, '0').slice(-4);
  return {
    hash: normalized,
    slug: `${WORDS[indexes[0]]}-${WORDS[indexes[1]]}-${WORDS[indexes[2]]}-${checksum}`,
    checksum,
  };
}

export function humanKeyForValue(value: string): HumanKey {
  return humanKeyFromHash(stableHash(value));
}
