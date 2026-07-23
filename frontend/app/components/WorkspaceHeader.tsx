import { Activity, Bell, CheckCheck, Database, RefreshCw } from "lucide-react";
import ThemeControl from "./ThemeControl";
import WorkspaceNav, { workspaceHref } from "./WorkspaceNav";

export type Health = {
  status: string;
  database: string;
  redis: string;
};

export type Notification = {
  id: string;
  category: string;
  severity: "info" | "warning" | "error";
  title: string;
  message: string;
  source_type: string;
  source_id: string;
  race_id?: string | null;
  race_date?: string | null;
  action_anchor?: string | null;
  is_read: boolean;
  read_at?: string | null;
  created_at: string;
};

export type NextAction = {
  label: string;
  detail: string;
  anchor: string;
};

type WorkspaceHeaderProps = {
  error: string;
  health: Health | null;
  isBusy: boolean;
  nextAction: NextAction;
  notificationCenterOpen: boolean;
  notifications: Notification[];
  onMarkAllNotificationsRead: () => void;
  onRefresh: () => void;
  onToggleNotificationCenter: () => void;
  onUpdateNotificationReadState: (notificationId: string, isRead: boolean) => void;
  progress: {
    python: boolean;
    independentAi: boolean;
    integration: boolean;
    bets: boolean;
    result: boolean;
  };
  routeAnchor: string;
  routeTitle: string;
  unreadNotificationCount: number;
};

export default function WorkspaceHeader({
  error,
  health,
  isBusy,
  nextAction,
  notificationCenterOpen,
  notifications,
  onMarkAllNotificationsRead,
  onRefresh,
  onToggleNotificationCenter,
  onUpdateNotificationReadState,
  progress,
  routeAnchor,
  routeTitle,
  unreadNotificationCount
}: WorkspaceHeaderProps) {
  return (
    <>
      <a className="skipLink" href={`#${routeAnchor}`}>主要操作へ移動</a>
      <header className="topBar">
        <div>
          <p className="eyebrow">Keiba AI Studio</p>
          <h1>{routeTitle}</h1>
        </div>
        <div className="statusStrip">
          <span className={health?.status === "ok" ? "status ok" : "status warn"}>
            <Activity size={16} aria-hidden="true" />
            API {health?.status || "loading"}
          </span>
          <span className="status">
            <Database size={16} aria-hidden="true" />
            DB {health?.database || "-"}
          </span>
          <button
            aria-expanded={notificationCenterOpen}
            aria-controls="notification-center"
            className="notificationButton"
            type="button"
            onClick={onToggleNotificationCenter}
            title="通知センター"
          >
            <Bell size={17} aria-hidden="true" />
            通知
            {unreadNotificationCount > 0 && (
              <span className="notificationBadge" aria-label={`未読${unreadNotificationCount}件`}>
                {unreadNotificationCount > 99 ? "99+" : unreadNotificationCount}
              </span>
            )}
          </button>
          <ThemeControl />
          <button className="iconButton" type="button" onClick={onRefresh} title="再読み込み">
            <RefreshCw size={18} aria-hidden="true" />
          </button>
        </div>
      </header>

      {notificationCenterOpen && (
        <section className="notificationCenter" id="notification-center" aria-label="通知センター">
          <div className="notificationCenterHeader">
            <div>
              <h2>通知センター</h2>
              <span>失敗ジョブとデータ品質警告の履歴</span>
            </div>
            <button
              disabled={unreadNotificationCount === 0}
              onClick={onMarkAllNotificationsRead}
              type="button"
            >
              <CheckCheck size={16} aria-hidden="true" />
              すべて既読
            </button>
          </div>
          {notifications.length === 0 ? (
            <p className="notificationEmpty">現在、通知はありません。</p>
          ) : (
            <ol className="notificationList">
              {notifications.map((notification) => (
                <li
                  className={`notificationItem ${notification.severity} ${notification.is_read ? "read" : "unread"}`}
                  key={notification.id}
                >
                  <div>
                    <strong>{notification.title}</strong>
                    <p>{notification.message}</p>
                    <small>
                      {new Date(notification.created_at).toLocaleString("ja-JP")}
                      {notification.race_id ? `｜レース ${notification.race_id}` : ""}
                    </small>
                  </div>
                  <div className="notificationActions">
                    {notification.action_anchor && (
                      <a href={workspaceHref(notification.action_anchor)}>確認する</a>
                    )}
                    <button
                      onClick={() => onUpdateNotificationReadState(notification.id, !notification.is_read)}
                      type="button"
                    >
                      {notification.is_read ? "未読に戻す" : "既読"}
                    </button>
                  </div>
                </li>
              ))}
            </ol>
          )}
        </section>
      )}

      <WorkspaceNav />

      <section className="nextActionCard" aria-live="polite">
        <div>
          <span>次に行う操作</span>
          <strong>{nextAction.label}</strong>
          <p>{nextAction.detail}</p>
        </div>
        <a href={workspaceHref(nextAction.anchor)}>対象画面へ</a>
        <ol aria-label="処理順">
          <li className={progress.python ? "done" : "current"}>Python</li>
          <li className={progress.independentAi ? "done" : ""}>独立AI</li>
          <li className={progress.integration ? "done" : ""}>比較・統合</li>
          <li className={progress.bets ? "done" : ""}>候補</li>
          <li className={progress.result ? "done" : ""}>結果</li>
        </ol>
      </section>

      {error && (
        <div className="errorBanner" role="alert">
          <strong>処理を完了できませんでした</strong>
          <span>{error}</span>
          <small>対象レースの品質・ジョブ失敗理由・APIキー設定を確認し、原因を解消してから再実行してください。</small>
        </div>
      )}

      {isBusy && <div className="busyBanner" role="status">処理中です。完了までこの画面を閉じずにお待ちください。</div>}
    </>
  );
}
