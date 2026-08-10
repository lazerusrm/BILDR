#!/usr/bin/env bash
set -euo pipefail

metadata=$(tr '[:upper:]' '[:lower:]')

blocked_names='(^|[^[:alnum:]_])(codex|grok|muse|claude|chatgpt|copilot|gemini|cursor|windsurf|devin|aider|openai|anthropic|deepseek|perplexity|qwen|kimi|gpt|llm)([^[:alnum:]_]|$)'
blocked_attribution='(generated|authored|written|assisted|created|committed)[[:space:]-]+by[[:space:]]+(an?[[:space:]]+)?(ai|bot|agent|model|tool)|with[[:space:]]+(the[[:space:]]+)?(help|assistance)[[:space:]]+of[[:space:]]+(an?[[:space:]]+)?(ai|bot|agent|model|tool)|co-authored-by:.*(bot|agent|model|ai)'

if grep -Eiq -e "$blocked_names" -e "$blocked_attribution" <<<"$metadata"; then
  echo "::error::Public change metadata contains prohibited automation attribution language."
  exit 1
fi

echo "Public change metadata policy passed."
