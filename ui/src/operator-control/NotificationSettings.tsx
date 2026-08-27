import { useCallback, useEffect, useState } from "react";
import { api } from "../api";
import type {
  NotificationDeliveryHealth,
  OperatorPresence,
  OperatorPresenceMode,
} from "../types";

const MODES: OperatorPresenceMode[] = ["interactive", "focus", "unattended"];

/**
 * Local notification presentation preference and the read-only integrity health
 * of in-product claims. Neither changes controller authority.
 */
export function NotificationSettings() {
  const [presence, setPresence] = useState<OperatorPresence>();
  const [health, setHealth] = useState<NotificationDeliveryHealth>();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  const load = useCallback(async () => {
    try {
      const [nextPresence, nextHealth] = await Promise.all([
        api.operatorPresence(),
        api.notificationDeliveryHealth(),
      ]);
      setPresence(nextPresence);
      setHealth(nextHealth);
      setError("");
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Unavailable");
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const choose = async (mode: OperatorPresenceMode) => {
    setBusy(true);
    try {
      setPresence(await api.setOperatorPresence(mode, presence?.version ?? 0));
      setError("");
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not set presence");
    } finally {
      setBusy(false);
    }
  };

  if (error && !presence) return null;

  return (
    <>
      <div className="settings-section-title">Notifications</div>
      <div className="settings-card">
        <div>
          <strong>Presence</strong>
          <span>
            How in-product claims are presented on this computer. It never
            changes controller authority.
          </span>
        </div>
        <div className="presence-choice">
          {MODES.map((mode) => (
            <button
              key={mode}
              className={`button ${presence?.mode === mode ? "primary" : "subtle"}`}
              onClick={() => void choose(mode)}
              disabled={busy}
            >
              {mode}
            </button>
          ))}
        </div>
      </div>
      {health && (
        <div className="settings-card">
          <div>
            <strong>Delivery health</strong>
            <span>
              {health.presented_examined_revisions} of{" "}
              {health.examined_current_revisions} current attention revisions
              rendered in this product
              {health.unpresented_action_required_examined_revisions > 0
                ? ` · ${health.unpresented_action_required_examined_revisions} action-required not yet shown`
                : ""}
              .
            </span>
          </div>
        </div>
      )}
      {error && <p className="settings-note">{error}</p>}
    </>
  );
}
