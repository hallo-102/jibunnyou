import { AlertTriangle, RefreshCw } from "lucide-react";

export type Job = {
  id: string;
  job_type: string;
  status: string;
  race_date?: string | null;
  race_id?: string | null;
  message?: string | null;
  created_at: string;
};

export type CollectionRun = {
  id: string;
  job_run_id: string;
  source_code: string;
  data_kind: string;
  status: string;
  mode: string;
  race_date?: string | null;
  race_id?: string | null;
  cache_hit: boolean;
  attempt_count: number;
  retry_count: number;
  request_count: number;
  quality_status?: string | null;
  error_code?: string | null;
  error_message?: string | null;
  created_at: string;
};

export type Issue = {
  id: number;
  severity: string;
  code: string;
  message: string;
  source_file?: string | null;
  race_id?: string | null;
  row_number?: number | null;
};

type OperationsPanelsProps = {
  collections: CollectionRun[];
  isBusy: boolean;
  issues: Issue[];
  jobs: Job[];
  onRetryCollection: (jobRunId: string) => Promise<void>;
};

export default function OperationsPanels({
  collections,
  isBusy,
  issues,
  jobs,
  onRetryCollection
}: OperationsPanelsProps) {
  return (
    <>
      <section data-route-section="races operations">
        <div className="sectionHeader">
          <h2>取得状況</h2>
          <span>{collections.length}</span>
        </div>
        <div className="logList">
          {collections.slice(0, 8).map((collection) => (
            <div key={collection.id} className="collectionRow">
              <div className="collectionMain">
                <span className={`pill ${collection.status}`}>{collection.status}</span>
                <strong>{collection.data_kind}</strong>
                <small>{collection.source_code}</small>
                <button
                  disabled={isBusy}
                  onClick={() => void onRetryCollection(collection.job_run_id)}
                  title="同じ対象を強制再取得"
                  type="button"
                >
                  <RefreshCw size={14} aria-hidden="true" />
                  再取得
                </button>
              </div>
              <div className="collectionMeta">
                <span>品質 {collection.quality_status || "-"}</span>
                <span>{collection.cache_hit ? "cache hit" : `試行 ${collection.attempt_count}`}</span>
                <span>retry {collection.retry_count}</span>
                <span>{collection.mode}</span>
              </div>
              {collection.error_message && (
                <p className="collectionError">
                  {collection.error_code || "COLLECTION_ERROR"}: {collection.error_message}
                </p>
              )}
            </div>
          ))}
          {collections.length === 0 && (
            <div className="emptyState">取得履歴はまだありません</div>
          )}
        </div>
      </section>

      <section data-route-section="operations" id="operations">
        <div className="sectionHeader">
          <h2>ジョブ</h2>
          <span>{jobs.length}</span>
        </div>
        <div className="logList">
          {jobs.slice(0, 8).map((job) => (
            <div key={job.id} className="logRow">
              <span className={`pill ${job.status}`}>{job.status}</span>
              <strong>{job.job_type}</strong>
              <small>{new Date(job.created_at).toLocaleString("ja-JP")}</small>
              {job.status === "failed" && job.message && (
                <small title={job.message}>失敗理由: {job.message}</small>
              )}
            </div>
          ))}
        </div>
      </section>

      <section data-route-section="operations">
        <div className="sectionHeader">
          <h2>品質チェック</h2>
          <AlertTriangle size={16} aria-hidden="true" />
        </div>
        <div className="logList">
          {issues.slice(0, 8).map((issue) => (
            <div key={issue.id} className="issueRow">
              <span className={`pill ${issue.severity}`}>{issue.severity}</span>
              <strong>{issue.code}</strong>
              <small>{issue.message}</small>
            </div>
          ))}
        </div>
      </section>
    </>
  );
}
