#!/usr/bin/env bash
# Vendored from https://github.com/plone/plone-doc-style-skill (version 0.5.0, MIT license).
# Do not edit here; update from upstream.
# check-prose.sh: list lines in MyST or Markdown pages that break the Plone documentation style rules.
#
# Usage: bash check-prose.sh [--summary] FILE...
#
# Each finding is printed as  file:line: rule: text  so that an agent or an editor can jump to it.
# The exit status is 1 when anything was found and 0 when every file is clean, which makes the
# script usable as a validation step: run it, fix each line, run it again until it exits 0.
#
# Rules (prose lines only, code fences and YAML frontmatter are skipped, admonition bodies are prose):
#   two-sentences   two sentences on one line (Plone writes one sentence per line)
#   comma-so        ", so" joining two clauses: split into two sentences
#   semicolon       two clauses joined with a semicolon: split into two sentences
#   contrast        "X, not Y" or "not only ... but": state the positive half
#   long-sentence   more than 25 words: split
#   vocabulary      words that mark prose as machine-written (ensure, leverage, robust, simply, ...)
#   em-dash         an em dash or a spaced "--": use a comma, a colon, or a new sentence
#   stock-conclusion a section titled Conclusion, Summary, Next steps, or similar: end with the last fact instead
#   shell-prompt    a "$ " or "% " prompt inside a shell or bash code block
#   code-ellipsis   "..." inside a code block, which breaks highlighting
#   json-comment    a comment inside a json code block
#   meta-mismatch   html_meta "description" and "property=og:description" differ
#   h1-count        not exactly one H1 in the page
#
# Needs only a POSIX shell, awk, and no network.

summary=0
if [ "${1:-}" = "--summary" ]; then summary=1; shift; fi
if [ $# -eq 0 ]; then
  echo "usage: bash check-prose.sh [--summary] FILE..." >&2
  exit 2
fi

check_file() {
  awk -v file="$1" '
  function report(rule, text) {
    gsub(/^[ \t]+|[ \t]+$/, "", text)
    if (length(text) > 140) text = substr(text, 1, 137) "..."
    printf "%s:%d: %s: %s\n", file, FNR, rule, text
    hits++
  }
  function is_prose_directive(info) {
    return info ~ /^\{(note|tip|warning|important|seealso|admonition|danger|caution|hint|attention|error|versionadded|versionchanged|versionremoved|deprecated|tab-item|card|grid-item-card|dropdown)\}/
  }
  BEGIN { fd = 0; h1 = 0; hits = 0; front = 0 }
  FNR == 1 && $0 ~ /^---[ \t]*$/ { front = 1; next }
  front == 1 {
    if ($0 ~ /^---[ \t]*$/) { front = 2; next }
    if (match($0, /"description": *"/)) { d = substr($0, RSTART + RLENGTH); sub(/".*$/, "", d); desc = d }
    if (match($0, /"property=og:description": *"/)) { d = substr($0, RSTART + RLENGTH); sub(/".*$/, "", d); ogdesc = d }
    next
  }
  # fence handling
  match($0, /^[ \t]*(```+|~~~+)/) {
    run = substr($0, RSTART, RLENGTH); gsub(/[ \t]/, "", run)
    info = substr($0, RSTART + RLENGTH); gsub(/^[ \t]+|[ \t]+$/, "", info)
    if (fd > 0 && length(run) >= flen[fd] && substr(run, 1, 1) == fchar[fd] && info == "") {
      fd--
    } else {
      fd++; flen[fd] = length(run); fchar[fd] = substr(run, 1, 1)
      fcode[fd] = is_prose_directive(info) ? 0 : 1
      lang = info; sub(/^\{code-block\} */, "", lang); flang[fd] = tolower(lang)
    }
    next
  }
  fd > 0 && fcode[fd] == 1 {
    l = flang[fd]
    if ((l == "shell" || l == "bash" || l == "sh") && $0 ~ /^[ \t]*[$%] /) report("shell-prompt", $0)
    if ($0 ~ /\.\.\./ || index($0, "\342\200\246")) report("code-ellipsis", $0)
    if (l == "json" && $0 ~ /^[ \t]*(\/\/|\/\*)/) report("json-comment", $0)
    next
  }
  # prose from here on
  /^[ \t]*$/ { next }
  /^\(.*\)=[ \t]*$/ { next }
  /^[ \t]*:[a-zA-Z_-]+:/ { next }
  /^[ \t]*\|/ { next }
  /^[ \t]*</ { next }
  /^[ \t]*\+\+\+[ \t]*$/ { next }
  /^# / { h1++; next }
  /^#+ +(Conclusion|Conclusions|Summary|In summary|Wrap-up|Wrapping up|Final thoughts|Next steps|Key takeaways)[ \t]*$/ { report("stock-conclusion", $0); next }
  /^#+ / { next }
  {
    line = $0
    sub(/^[ \t]*(> )?/, "", line)
    sub(/^([-*+]|[0-9]+\.)[ \t]+/, "", line)
    gsub(/`[^`]*`/, "`x`", line)
    gsub(/"[^"]*"/, "\"x\"", line)
    # two sentences on one line, allowing common abbreviations
    if (match(line, /[a-z0-9)\]][.!?] +[A-Z]/)) {
      before = substr(line, 1, RSTART)
      n = split(before, w, /[ \t]+/); last = tolower(w[n])
      if (last !~ /^(e\.g|i\.e|vs|etc|no|fig|approx|ca)$/) report("two-sentences", $0)
    }
    if (line ~ /, so( |$)/) report("comma-so", $0)
    if (line ~ /; / || line ~ /;$/) report("semicolon", $0)
    if (line ~ /, not [a-z]/ || line ~ /(^|[^a-zA-Z])[Nn]ot only[^a-zA-Z]/) report("contrast", $0)
    if (index(line, "\342\200\224") || line ~ / -- ([^>]|$)/ || line ~ / --$/) report("em-dash", $0)
    nw = split(line, words, /[ \t]+/)
    if (nw > 25) report("long-sentence", "(" nw " words) " $0)
    low = tolower(line)
    if (match(low, /(^|[^a-z])(ensur(e|es|ed|ing)|leverag(e|es|ed|ing)|utiliz(e|es|ed|ing)|seamless(ly)?|robust(ly|ness)?|comprehensive(ly)?|crucial(ly)?|delv(e|es|ed|ing)|streamlin(e|es|ed|ing)|foster(s|ed|ing)?|enhanc(e|es|ed|ing|ement|ements)|simply|easily|trivial(ly)?)([^a-z]|$)/)) {
      report("vocabulary", $0)
    } else if (low ~ /worth noting|important to note|in order to/ || line ~ /^(Additionally|Furthermore|Moreover)[ ,]/) {
      report("vocabulary", $0)
    }
  }
  END {
    if (front == 2 && desc != "" && ogdesc != "" && desc != ogdesc) report("meta-mismatch", "description and og:description differ")
    if (h1 != 1) report("h1-count", h1 " H1 headings found, expected 1")
    exit hits > 0 ? 1 : 0
  }' "$1"
}

status=0
tmp=$(mktemp)
for f in "$@"; do
  if [ ! -f "$f" ]; then echo "$f: not a file" >&2; status=1; continue; fi
  check_file "$f" >> "$tmp" || status=1
done
total=$(wc -l < "$tmp" | tr -d ' ')
if [ "$summary" -eq 0 ]; then cat "$tmp"; fi
if [ "$total" -gt 0 ]; then
  echo
  echo "findings by rule:"
  awk -F': ' '{ print $2 }' "$tmp" | sort | uniq -c | sort -rn | awk '{ printf "  %4d  %s\n", $1, $2 }'
  echo "  $total total in $# file(s)"
else
  echo "clean: $# file(s), no findings"
fi
rm -f "$tmp"
exit $status
