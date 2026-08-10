#!/usr/bin/env bash
set -euo pipefail

shopt -s nocasematch
while IFS= read -r path; do
  [[ -e "$path" ]] || continue
  case "${path##*/}" in
    agents.md|claude.md|grok.md|codex.md)
      echo "::error::The public tree contains a prohibited repository instruction file."
      exit 1
      ;;
  esac
done < <(git ls-files --cached --others --exclude-standard)

echo "Public repository file policy passed."
