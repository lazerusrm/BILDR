-- Bind command and file-change approvals to the complete mutable worktree
-- snapshot, not only to the commit at HEAD.
ALTER TABLE approvals ADD COLUMN expected_worktree_fingerprint TEXT;
