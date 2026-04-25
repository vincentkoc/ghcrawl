import { getLatestClusterRun } from '../cluster/run-queries.js';
import type { GitcrawlConfig } from '../config.js';
import type { SqliteDatabase } from '../db/sqlite.js';
import { getEmbeddingWorkset } from '../embedding/workset.js';
import type { TuiRepoStats } from '../service-types.js';

export function getTuiRepoStats(params: { db: SqliteDatabase; config: GitcrawlConfig; repoId: number }): TuiRepoStats {
  const counts = params.db
    .prepare(
      `select kind, count(*) as count
       from threads
       where repo_id = ? and state = 'open' and closed_at_local is null
       group by kind`,
    )
    .all(params.repoId) as Array<{ kind: 'issue' | 'pull_request'; count: number }>;
  const latestRun = getLatestClusterRun(params.db, params.repoId);
  const latestSync =
    (params.db
      .prepare("select finished_at from sync_runs where repo_id = ? and status = 'completed' order by id desc limit 1")
      .get(params.repoId) as { finished_at: string | null } | undefined) ?? null;
  const latestEmbed =
    (params.db
      .prepare("select finished_at from embedding_runs where repo_id = ? and status = 'completed' order by id desc limit 1")
      .get(params.repoId) as { finished_at: string | null } | undefined) ?? null;
  const embeddingWorkset = getEmbeddingWorkset({ db: params.db, config: params.config, repoId: params.repoId });
  const staleThreadIds = new Set<number>(embeddingWorkset.pending.map((task) => task.threadId));
  return {
    openIssueCount: counts.find((row) => row.kind === 'issue')?.count ?? 0,
    openPullRequestCount: counts.find((row) => row.kind === 'pull_request')?.count ?? 0,
    lastGithubReconciliationAt: latestSync?.finished_at ?? null,
    lastEmbedRefreshAt: latestEmbed?.finished_at ?? null,
    staleEmbedThreadCount: staleThreadIds.size,
    staleEmbedSourceCount: embeddingWorkset.pending.length,
    latestClusterRunId: latestRun?.id ?? null,
    latestClusterRunFinishedAt: latestRun?.finished_at ?? null,
  };
}
