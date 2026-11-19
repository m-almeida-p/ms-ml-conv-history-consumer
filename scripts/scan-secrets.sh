#!/usr/bin/env bash
# Looks for the REAL secret value of .env (the GitHub token Karavan uses)
# in every tracked file and in everything that a push would send, and prints only COUNTS, never a value.
#
#   ./scripts/scan-secrets.sh            exit 0 when nothing is found, 1 when something is
#
# Run it before every push. It looks for the values, not for patterns, so it cannot be fooled by a different format. It does not
# look at .env itself (git-ignored) or at untracked files, because they are not pushed. Needs .env and git.

# shellcheck source=./common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
cd "${REPO_DIR}"
[ -f .env ] || { echo "❌  no .env: there is nothing to look for" >&2; exit 2; }

NAMES=(GITHUB_MALMEIDAP_PAT)
base="origin/main"; git rev-parse --verify -q "${base}" >/dev/null || base="HEAD"
total=0; checked=0
for name in "${NAMES[@]}"; do
  value="$( { grep -E "^${name}=" .env || true; } | head -1 | cut -d= -f2-)"
  value="${value%\"}"; value="${value#\"}"
  [ -n "${value}" ] || continue
  checked=$((checked + 1))
  in_files="$( { git ls-files -z | xargs -0 grep -lF -- "${value}" 2>/dev/null || true; } | wc -l | tr -d ' ')"
  in_push="$( { git diff "${base}..HEAD" | grep -cF -- "${value}"; } || true)"
  printf '%-26s tracked files: %s   unpushed diff lines: %s\n' "${name}" "${in_files}" "${in_push}"
  total=$((total + in_files + in_push))
done
[ "${checked}" -gt 0 ] || { echo "❌  .env has none of the values to look for" >&2; exit 2; }
if [ "${total}" = 0 ]; then echo "✅  ${checked} secret value(s) searched: no match in tracked files or in what a push would send"; else echo "❌  ${total} match(es): do not push"; exit 1; fi
