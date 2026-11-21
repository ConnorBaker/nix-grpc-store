#!/usr/bin/env bash
# Compare INTEGRATION.md's rows with a remote: each branch's head there and
# whether the head this branch merged is in HEAD.  Read-only.
set -u
remote=${1:-origin}
cd "$(git rev-parse --show-toplevel)"
printf '%-45s %-12s %-12s %s\n' branch merged remote in-HEAD
grep -E '^\| [0-9]+ \|' INTEGRATION.md | while IFS='|' read -r _ n b h _; do
  b=$(echo "$b" | tr -d ' `'); h=$(echo "$h" | tr -d ' `')
  r=$(git ls-remote "$remote" "refs/heads/$b" | cut -c1-12)
  if git merge-base --is-ancestor "$h" HEAD 2>/dev/null; then m=yes; else m=NO; fi
  s=same; [ -z "$r" ] && s=absent; [ -n "$r" ] && [ "$r" != "${h:0:12}" ] && s="moved:$r"
  printf '%-45s %-12s %-12s %s\n' "$b" "${h:0:12}" "$s" "$m"
done
