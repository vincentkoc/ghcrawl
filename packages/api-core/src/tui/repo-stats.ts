import { getLatestClusterRun } from '../cluster/run-queries.js';
import type { GitcrawlConfig } from '../config.js';
import type { SqliteDatabase } from '../db/sqlite.js';
import { getEmbeddingWorkset } from '../embedding/workset.js';
import type { TuiRefreshState, TuiRepoStats } from '../service-types.js';

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

export function getTuiRepositoryRefreshState(params: {
  db: SqliteDatabase;
  repository: { id: number; updatedAt: string };
}): TuiRefreshState {
  const threadState = params.db
    .prepare(
      `select
         max(updated_at) as thread_updated_at,
         max(closed_at_local) as thread_closed_at
       from threads
       where repo_id = ?`,
    )
    .get(params.repository.id) as { thread_updated_at: string | null; thread_closed_at: string | null };
  const clusterState = params.db
    .prepare(
      `select max(closed_at_local) as cluster_closed_at
       from clusters
       where repo_id = ?`,
    )
    .get(params.repository.id) as { cluster_closed_at: string | null };
  const durableClusterState = params.db
    .prepare(
      `select max(updated_at) as durable_cluster_updated_at
       from cluster_groups
       where repo_id = ?`,
    )
    .get(params.repository.id) as { durable_cluster_updated_at: string | null };
  const durableMembershipState = params.db
    .prepare(
      `select max(cm.updated_at) as durable_membership_updated_at
       from cluster_memberships cm
       join cluster_groups cg on cg.id = cm.cluster_id
       where cg.repo_id = ?`,
    )
    .get(params.repository.id) as { durable_membership_updated_at: string | null };
  const latestSync = params.db
    .prepare("select id from sync_runs where repo_id = ? and status = 'completed' order by id desc limit 1")
    .get(params.repository.id) as { id: number } | undefined;
  const latestEmbedding = params.db
    .prepare("select id from embedding_runs where repo_id = ? and status = 'completed' order by id desc limit 1")
    .get(params.repository.id) as { id: number } | undefined;
  const latestClusterRun = getLatestClusterRun(params.db, params.repository.id);

  return {
    repositoryUpdatedAt: params.repository.updatedAt,
    threadUpdatedAt: threadState.thread_updated_at,
    threadClosedAt: threadState.thread_closed_at,
    clusterClosedAt: clusterState.cluster_closed_at,
    durableClusterUpdatedAt: durableClusterState.durable_cluster_updated_at,
    durableMembershipUpdatedAt: durableMembershipState.durable_membership_updated_at,
    latestSyncRunId: latestSync?.id ?? null,
    latestEmbeddingRunId: latestEmbedding?.id ?? null,
    latestClusterRunId: latestClusterRun?.id ?? null,
  };
}
