-- Preserve a task-launch failure even when no attempt could be created.
-- Attempt-level failures remain authoritative once an attempt exists.
ALTER TABLE tasks ADD COLUMN failure_reason TEXT;
