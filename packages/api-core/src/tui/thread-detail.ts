import type { SqliteDatabase } from '../db/sqlite.js';
import { SUMMARY_PROMPT_VERSION } from '../service-constants.js';
import type { TuiThreadDetail } from '../service-types.js';
import { normalizeKeySummaryDisplayText } from '../service-utils.js';

export function getTuiThreadSummaries(db: SqliteDatabase, threadId: number, summaryModel: string): TuiThreadDetail['summaries'] {
  const rows = db
    .prepare(
      `select summary_kind, summary_text
       from document_summaries
       where thread_id = ? and model = ? and prompt_version = ?
       order by summary_kind asc`,
    )
    .all(threadId, summaryModel, SUMMARY_PROMPT_VERSION) as Array<{ summary_kind: string; summary_text: string }>;
  const summaries: TuiThreadDetail['summaries'] = {};
  for (const summary of rows) {
    if (
      summary.summary_kind === 'problem_summary' ||
      summary.summary_kind === 'solution_summary' ||
      summary.summary_kind === 'maintainer_signal_summary' ||
      summary.summary_kind === 'dedupe_summary'
    ) {
      summaries[summary.summary_kind] = summary.summary_text;
    }
  }
  return summaries;
}

export function getLatestTuiKeySummary(db: SqliteDatabase, threadId: number, summaryModel: string): TuiThreadDetail['keySummary'] {
  const row = db
    .prepare(
      `select ks.summary_kind, ks.prompt_version, ks.model, ks.key_text
       from thread_key_summaries ks
       join thread_revisions tr on tr.id = ks.thread_revision_id
       where tr.thread_id = ?
         and ks.summary_kind = 'llm_key_3line'
       order by
         case when ks.model = ? then 0 else 1 end,
         tr.id desc,
         ks.created_at desc
       limit 1`,
    )
    .get(threadId, summaryModel) as
    | {
        summary_kind: string;
        prompt_version: string;
        model: string;
        key_text: string;
      }
    | undefined;
  if (!row) return null;
  const text = normalizeKeySummaryDisplayText(row.key_text);
  if (!text) return null;
  return {
    summaryKind: row.summary_kind,
    promptVersion: row.prompt_version,
    model: row.model,
    text,
  };
}

export function getTopChangedFiles(db: SqliteDatabase, threadId: number, limit: number): TuiThreadDetail['topFiles'] {
  const latestRevision = db
    .prepare(
      `select id
       from thread_revisions
       where thread_id = ?
       order by id desc
       limit 1`,
    )
    .get(threadId) as { id: number } | undefined;
  if (!latestRevision) return [];

  return db
    .prepare(
      `select cf.path, cf.status, cf.additions, cf.deletions
       from thread_code_snapshots cs
       join thread_changed_files cf on cf.snapshot_id = cs.id
       where cs.thread_revision_id = ?
       order by (cf.additions + cf.deletions) desc, cf.path asc
       limit ?`,
    )
    .all(latestRevision.id, limit) as TuiThreadDetail['topFiles'];
}
