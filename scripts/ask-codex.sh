#!/usr/bin/env bash
# ask-codex — hand Codex a prompt from a file (or stdin) instead of fighting
# shell quoting.
#
# `codex exec "..."` inline is a trap: any prompt long enough to be useful
# contains quotes, backticks, $, parentheses and newlines, and zsh mangles at
# least one of them. This reads the prompt as data.
#
#   ask-codex.sh review-prompt.md                  # prompt from a file
#   echo "why is this slow?" | ask-codex.sh        # prompt from stdin
#   ask-codex.sh -C ~/dev/other-repo prompt.md     # run in another repo
#   ask-codex.sh -o out.md prompt.md               # tee the transcript
#
# Model and reasoning effort come from ~/.codex/config.toml (currently
# gpt-6-astra / xhigh). Override per-call with CODEX_MODEL / CODEX_EFFORT.
#
# Exit status is Codex's own. The full transcript goes to stdout; with -o it
# is also saved, and the final answer alone is echoed at the end.
set -euo pipefail

usage() {
    sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
    exit "${1:-0}"
}

workdir="$PWD"
outfile=""
while getopts ":C:o:h" opt; do
    case "$opt" in
        C) workdir="$OPTARG" ;;
        o) outfile="$OPTARG" ;;
        h) usage 0 ;;
        \?) echo "ask-codex: unknown option -$OPTARG" >&2; usage 2 ;;
        :)  echo "ask-codex: -$OPTARG needs an argument" >&2; usage 2 ;;
    esac
done
shift $((OPTIND - 1))

command -v codex >/dev/null 2>&1 || {
    echo "ask-codex: codex is not on PATH" >&2
    exit 127
}

# Prompt: first positional arg is a file; otherwise read stdin.
if [[ $# -gt 0 ]]; then
    [[ -r "$1" ]] || { echo "ask-codex: cannot read prompt file: $1" >&2; exit 2; }
    prompt_file="$1"
    cleanup=""
elif [[ ! -t 0 ]]; then
    prompt_file="$(mktemp -t codex-prompt)"
    cleanup="$prompt_file"
    cat > "$prompt_file"
else
    echo "ask-codex: no prompt (pass a file or pipe stdin)" >&2
    usage 2
fi
# shellcheck disable=SC2064  # expand now: the path must be fixed at trap time
[[ -n "$cleanup" ]] && trap "rm -f '$cleanup'" EXIT

[[ -s "$prompt_file" ]] || { echo "ask-codex: prompt is empty" >&2; exit 2; }

args=(exec)
[[ -n "${CODEX_MODEL:-}" ]]  && args+=(-c "model=\"$CODEX_MODEL\"")
[[ -n "${CODEX_EFFORT:-}" ]] && args+=(-c "model_reasoning_effort=\"$CODEX_EFFORT\"")

# `-` makes codex read the prompt from stdin, so nothing passes through the
# shell's own parsing.
run() { (cd "$workdir" && codex "${args[@]}" - < "$prompt_file"); }

if [[ -n "$outfile" ]]; then
    run | tee "$outfile"
    status=${PIPESTATUS[0]}
    echo >&2
    echo "ask-codex: transcript saved to $outfile" >&2
else
    run
    status=$?
fi
exit "$status"
