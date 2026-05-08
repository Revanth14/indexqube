export const secretPrefix = 'indexqube.providerKey.';
export const sessionKeyName = 'indexqube.sessionKey';
export const usageTotalsKey = 'indexqube.usageTotals.v1';

export const sensitiveFilePatterns = [
    '.env',
    '.env.local',
    '.env.development',
    '.env.production',
    '.npmrc',
    '.pypirc',
    '.netrc',
    'id_rsa',
    'id_dsa',
    'id_ecdsa',
    'id_ed25519'
];

export const sensitiveExtensions = new Set([
    '.pem',
    '.key',
    '.crt',
    '.cer',
    '.p12',
    '.pfx',
    '.jks',
    '.keystore'
]);

export const noisyLockFiles = new Set([
    'package-lock.json',
    'pnpm-lock.yaml',
    'yarn.lock',
    'bun.lockb',
    'poetry.lock',
    'Pipfile.lock',
    'Cargo.lock'
]);

export const generatedPathSegments = new Set([
    '.git',
    'node_modules',
    'dist',
    'bin',
    'out',
    'build',
    'vendor',
    '.cache',
    'coverage',
    'tmp',
    'temp',
    '.next',
    '.nuxt',
    'target',
    '.turbo',
    '.venv',
    'venv',
    '__pycache__'
]);

export const builtinSecretPatterns: Array<{ name: string; pattern: RegExp }> = [
    { name: 'private key', pattern: /-----BEGIN (?:[A-Z ]+ )?PRIVATE KEY-----[\s\S]*?-----END (?:[A-Z ]+ )?PRIVATE KEY-----/ },
    { name: 'OpenAI-style API key', pattern: /\bsk-[A-Za-z0-9_-]{20,}\b/ },
    { name: 'GitHub token', pattern: /\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9_]{20,}\b/ },
    { name: 'GitHub fine-grained token', pattern: /\bgithub_pat_[A-Za-z0-9_]{30,}\b/ },
    { name: 'AWS access key', pattern: /\bAKIA[0-9A-Z]{16}\b/ },
    { name: 'Slack token', pattern: /\bxox[baprs]-[A-Za-z0-9-]{20,}\b/ },
    { name: 'JWT-like token', pattern: /\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b/ }
];
