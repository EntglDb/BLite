#!/usr/bin/env bash
# BLite Native AOT smoke test. Local only, never run in CI (a publish needs clang and minutes).
#
#   tools/aot-smoke/run.sh [--seed N] [--steps N] [--keep]
#   tools/aot-smoke/run.sh --prove-seeds [name filter]
#
# Runs the Consumer under JIT and as a Native AOT binary for the host RID against fresh temp
# databases with the same seed, compares the two dumps section by section, counts the trim and AOT
# warnings of the publish by assembly, publishes Consumer.Linq and checks that every marked call of
# an annotated API warns at its own line. One verdict line per check; exit code 1 on any failure
# that is not a candidate registered in tools/aot-smoke/candidates.md, 2 if a prerequisite is
# missing. --prove-seeds applies each tools/aot-smoke/seeds/*.patch in a scratch git worktree of
# HEAD, runs this script there and requires it to fail (CAUGHT) rather than pass (MISSED).
#
# Requires: the .NET SDK of the repository, clang and a linker (on macOS the Xcode command line
# tools), and the ILCompiler pack (restored from NuGet, or shipped with a source-built SDK).

set -u -o pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
root="$(git -C "$here" rev-parse --show-toplevel)"
export DOTNET_CLI_UI_LANGUAGE=en DOTNET_NOLOGO=1 DOTNET_CLI_TELEMETRY_OPTOUT=1

seed=1
steps=300
keep=0
prove=""
proving=0
while [ $# -gt 0 ]; do
    case "$1" in
        --seed) seed="$2"; shift 2 ;;
        --steps) steps="$2"; shift 2 ;;
        --keep) keep=1; shift ;;
        --prove-seeds) proving=1; prove="${2:-}"; [ $# -gt 1 ] && shift; shift ;;
        -h|--help) sed -n '2,17p' "$0"; exit 0 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

now() { perl -MTime::HiRes=time -e 'printf "%.1f", time'; }
elapsed() { perl -e "printf '%.1f', $2 - $1"; }

# ── --prove-seeds: each seed must make this script fail ───────────────────────

if [ "$proving" = 1 ]; then
    status=0
    found=0
    for patch in "$here"/seeds/*.patch; do
        name="$(basename "$patch")"
        case "$name" in *"$prove"*) ;; *) continue ;; esac
        found=1
        # The physical path: a "//" or a symlinked temp directory (/var -> /private/var on macOS)
        # gives MSBuild two spellings of one project, and restore then loses BLite.Caching's packages.
        scratch="$(cd "$(mktemp -d "${TMPDIR:-/tmp}/blite-aot-seed.XXXXXX")" && pwd -P)"
        rmdir "$scratch"
        if ! git -C "$root" worktree add --detach "$scratch" HEAD >/dev/null 2>&1; then
            echo "ERROR   $name: worktree"; status=1; continue
        fi
        if ! git -C "$scratch" apply --ignore-whitespace "$patch" 2>/dev/null; then
            echo "ERROR   $name: the seed does not apply to HEAD"; status=1
        else
            out="$("$scratch/tools/aot-smoke/run.sh" --seed "$seed" --steps "$steps" 2>&1)"
            code=$?
            failed="$(printf '%s\n' "$out" | grep -E '^(FAIL|DIFFERENT|MISSING|ERROR)' | head -3 | tr '\n' ';')"
            if printf '%s\n' "$out" | grep -qE '^(ERROR|MISSING)'; then code=2; fi   # a seed that breaks the build proves nothing
            case $code in
                1) echo "CAUGHT  $name: ${failed%;}" ;;
                0) echo "MISSED  $name: run.sh passed with the seed applied"; status=1 ;;
                *) echo "ERROR   $name: $(printf '%s\n' "$out" | grep -m1 -E '^(ERROR|MISSING)' || printf '%s\n' "$out" | tail -1)"; status=1 ;;
            esac
            printf '%s\n' "$out" > "${TMPDIR:-/tmp}/blite-aot-seed-$name.log"
        fi
        git -C "$root" worktree remove --force "$scratch" >/dev/null 2>&1
    done
    [ "$found" = 0 ] && { echo "no seed matches '$prove' in $here/seeds"; exit 1; }
    exit $status
fi

# ── Prerequisites ─────────────────────────────────────────────────────────────

failures=0
verdict() { echo "$*"; case "$*" in FAIL*|DIFFERENT*|MISSING*|ERROR*) failures=$((failures + 1)) ;; esac; }

command -v dotnet >/dev/null || { echo "MISSING dotnet: install the .NET SDK"; exit 2; }
rid="$(dotnet --info 2>/dev/null | awk '/^ *RID:/ { print $2; exit }')"
[ -n "$rid" ] || { echo "MISSING runtime identifier: 'dotnet --info' prints no RID"; exit 2; }

publish_args=(-r "$rid" -c Release -nologo)
case "$(uname -s)" in
    Darwin)
        # /usr/bin/clang is the xcrun shim, which passes the SDK; a clang earlier on PATH (swiftly,
        # Homebrew llvm) or Xcode's own binary called directly may not find the SDK's libraries.
        { xcrun --find clang && xcrun --show-sdk-path; } >/dev/null 2>&1 \
            || { echo "MISSING clang: install the Xcode command line tools (xcode-select --install)"; exit 2; }
        clang=/usr/bin/clang
        publish_args+=("-p:CppCompilerAndLinker=$clang")
        # The Homebrew build of the SDK links its NativeAOT runtime against Homebrew's OpenSSL and
        # Brotli without passing their library directories; point the linker at them.
        if command -v brew >/dev/null && dotnet --list-sdks | grep -q "$(brew --prefix 2>/dev/null)/Cellar/dotnet"; then
            prefix="$(brew --prefix)"
            for lib in openssl brotli; do
                [ -d "$prefix/opt/$lib/lib" ] && export LIBRARY_PATH="$prefix/opt/$lib/lib${LIBRARY_PATH:+:$LIBRARY_PATH}"
            done
        fi
        ;;
    *)
        clang="$(command -v clang || true)"
        [ -n "$clang" ] || { echo "MISSING clang: install clang and the platform linker (see the Native AOT prerequisites)"; exit 2; }
        ;;
esac

work="$(cd "$(mktemp -d "${TMPDIR:-/tmp}/blite-aot-smoke.XXXXXX")" && pwd -P)"
cleanup() { [ "$keep" = 1 ] && echo "work directory kept: $work" || rm -rf "$work"; }
trap cleanup EXIT

echo "BLite AOT smoke test: $(git -C "$root" rev-parse --short HEAD) on $rid, seed $seed, $steps steps, clang $clang"

# ── Registered candidates ─────────────────────────────────────────────────────
# Rows of the registry table in candidates.md: | A<n> | warning, linq-warning or dump | `substring` | ...

candidates="$here/candidates.md"
known() { # $1 = kinds (a regex: warning, linq-warning, dump), $2 = text; prints the first matching row's id
    [ -f "$candidates" ] || return 1
    awk -F'|' -v kind="$1" -v text="$2" '
        $2 ~ /^ *A[0-9]+ *$/ && $3 ~ "^ *(" kind ") *$" {
            id = $2; gsub(/ /, "", id)
            pat = $4; sub(/^[^`]*`/, "", pat); sub(/`[^`]*$/, "", pat)
            if (pat != "" && index(text, pat) > 0) { print id; exit }
        }' "$candidates"
}

# ── The generator copy ────────────────────────────────────────────────────────

drift=0
awk '/verbatim from StoreModel.cs: begin/ { n++; on = 1; next } /verbatim from StoreModel.cs: end/ { on = 0; next } on { print > (dir "/block" n) }' \
    dir="$work" "$here/Consumer/StoreModelGenerator.cs"
tr -d '\r' < "$root/tests/BLite.Tests/Oracles/StoreModel.cs" > "$work/StoreModel.cs"
for block in "$work"/block*; do
    tr -d '\r' < "$block" > "$block.lf"
    perl -0777 -e 'open F, $ARGV[0]; $b = <F>; open G, $ARGV[1]; $s = <G>; exit(index($s, $b) >= 0 ? 0 : 1)' "$block.lf" "$work/StoreModel.cs" || drift=1
done
[ "$drift" = 0 ] && verdict "SAME    generator: Consumer/StoreModelGenerator.cs matches tests/BLite.Tests/Oracles/StoreModel.cs" \
                 || verdict "FAIL    generator: Consumer/StoreModelGenerator.cs drifted from tests/BLite.Tests/Oracles/StoreModel.cs; copy it again"

# ── JIT run ───────────────────────────────────────────────────────────────────

t0=$(now)
if ! dotnet build "$here/Consumer" -c Release -nologo > "$work/build-jit.log" 2>&1; then
    verdict "ERROR   JIT build failed: $(grep -m1 -E 'error' "$work/build-jit.log")"; exit 1
fi
t1=$(now)
dotnet run --project "$here/Consumer" -c Release --no-build -- "$work/jit.txt" "$seed" "$steps" > "$work/run-jit.log" 2>&1
jit_exit=$?
t2=$(now)
[ -f "$work/jit.txt" ] || { verdict "ERROR   JIT run wrote no dump (exit $jit_exit): $(tail -1 "$work/run-jit.log")"; exit 1; }

# ── Native AOT publish and run ────────────────────────────────────────────────
# ILC prints its warnings only when it runs, and an incremental publish skips it when its inputs
# are unchanged; the native intermediates are removed so that every run sees the full analysis.

fresh_native() { rm -rf "$here/$1/obj/Release/net10.0/$rid/native"; }
fresh_native Consumer

if ! dotnet publish "$here/Consumer" "${publish_args[@]}" -o "$work/aot" > "$work/publish.log" 2>&1; then
    verdict "ERROR   Native AOT publish failed: $(grep -m1 -E 'error|ld: ' "$work/publish.log" | sed 's/^ *//')"
    grep -q -i 'ilcompiler' "$work/publish.log" && echo "        (is the Microsoft.DotNet.ILCompiler package restorable for $rid?)"
    exit 1
fi
t3=$(now)
"$work/aot/BLite.AotSmoke" "$work/aot.txt" "$seed" "$steps" > "$work/run-aot.log" 2>&1
aot_exit=$?
t4=$(now)
[ -f "$work/aot.txt" ] || { verdict "ERROR   AOT run wrote no dump (exit $aot_exit): $(tail -1 "$work/run-aot.log")"; exit 1; }

# ── Dump comparison, section by section ───────────────────────────────────────

split_sections() { awk -v dir="$2" '/^## / { f = sprintf("%s/%03d", dir, ++n) } n { print > f }' "$1"; }
mkdir -p "$work/jit" "$work/aotdump"
split_sections "$work/jit.txt" "$work/jit"
split_sections "$work/aot.txt" "$work/aotdump"

if cmp -s "$work/jit.txt" "$work/aot.txt"; then
    verdict "SAME    dump: JIT and Native AOT dumps are byte-identical ($(wc -l < "$work/jit.txt" | tr -d ' ') lines)"
else
    unknown=0
    for f in "$work/jit"/* "$work/aotdump"/*; do
        s="$(basename "$f")"
        title="$(head -1 "$f" | sed 's/^## //')"
        [ "$title" = "end" ] && continue   # the summary line counts the failed sections of each build
        [ -f "$work/jit/$s" ] && [ -f "$work/aotdump/$s" ] && cmp -s "$work/jit/$s" "$work/aotdump/$s" && continue
        [ "$f" = "$work/aotdump/$s" ] && [ -f "$work/jit/$s" ] && continue   # each pair once
        id=""
        while IFS= read -r line; do
            id="$(known dump "$line")" && [ -n "$id" ] && break
        done < <(cat "$work/jit/$s" "$work/aotdump/$s" 2>/dev/null | grep '^!!')
        first="$(diff "$work/jit/$s" "$work/aotdump/$s" 2>/dev/null | grep -m1 '^>' | cut -c3-160)"
        if [ -n "$id" ]; then
            echo "KNOWN   dump section '$title' differs [known candidate: $id]: $first"
        else
            unknown=1
            verdict "DIFFERENT dump section '$title': JIT $(grep -c '' "$work/jit/$s" 2>/dev/null) lines, AOT $(grep -c '' "$work/aotdump/$s" 2>/dev/null) lines; first AOT line that differs: $first"
        fi
    done
    [ "$unknown" = 0 ] && echo "SAME    dump: every difference between the JIT and the AOT dump is a registered candidate"
fi

# A section that fails the same way under both builds is no difference, but it is a failure.
for f in "$work/jit"/*; do
    s="$(basename "$f")"
    cmp -s "$f" "$work/aotdump/$s" || continue
    while IFS= read -r line; do
        id="$(known dump "$line")"
        if [ -n "$id" ]; then echo "KNOWN   both builds [known candidate: $id]: ${line#!! }" | cut -c1-240
        else verdict "FAIL    both builds: ${line#!! }"; fi
    done < <(grep '^!!' "$f")
done
[ "$jit_exit" = 0 ] || [ "$(grep -c '^!!' "$work/jit.txt")" -gt 0 ] || verdict "FAIL    JIT run exited $jit_exit without a failed section: $(tail -1 "$work/run-jit.log")"
[ "$aot_exit" = 0 ] || [ "$(grep -c '^!!' "$work/aot.txt")" -gt 0 ] || verdict "FAIL    AOT run exited $aot_exit without a failed section: $(tail -1 "$work/run-aot.log")"

# ── Warnings of the publish, by assembly ──────────────────────────────────────
# ILC prints "<file>(<line>): Trim analysis warning ILxxxx: <member>: <message>", or
# "ILC : ... warning ILxxxx: <member>: ..." when it has no source location.

ilc_warnings() { # $1 = publish log; one line per distinct ILC warning, the repository path removed
    grep -E '(Trim|AOT|Single-file) analysis warning IL[0-9]+|warning IL(2104|3053)' "$1" \
        | sed -E 's/ \[[^]]*\.csproj(::[^]]*)?\]$//; s/^ +//' | sed "s|$root/||g" | sort -u
}
assembly_of() { # $1 = warning line
    case "$1" in
        src/*) echo "$1" | cut -d/ -f2 ;;
        tools/aot-smoke/*/obj/*BLite.SourceGenerators*) echo "BLite.SourceGenerators (generated into the consumer)" ;;
        tools/aot-smoke/*) echo "consumer" ;;
        *"Assembly '"*) echo "$1" | sed -E "s/.*Assembly '([^']+)'.*/\\1/" ;;
        *) echo "$1" | sed -E 's/^.*warning IL[0-9]+: ([A-Za-z0-9_]+(\.[A-Za-z0-9_]+)?).*/\1/' ;;
    esac
}
count_warnings() { # $1 = warnings file, $2 = label, $3 = candidate kinds; per-assembly counts, checks BLite's
    local warnings="$1" label="$2" kinds="$3" total=0 blite=0 newcount=0 line asm id
    : > "$work/$label.assemblies"
    : > "$work/$label.known"
    while IFS= read -r line; do
        asm="$(assembly_of "$line")"
        echo "$asm" >> "$work/$label.assemblies"
        total=$((total + 1))
        case "$asm" in BLite*) ;; *) continue ;; esac
        blite=$((blite + 1))
        id="$(known "$kinds" "$line")"
        if [ -n "$id" ]; then echo "$id" >> "$work/$label.known"
        else newcount=$((newcount + 1)); echo "        new: $(echo "$line" | cut -c1-230)"; fi
    done < "$warnings"
    echo "        $label warnings by assembly: $(sort "$work/$label.assemblies" | uniq -c | awk '{ c = $1; $1 = ""; printf "%s%s=%d", sep, substr($0, 2), c; sep = ", " }')${total:+ (total $total)}"
    local knownlist; knownlist="$(sort "$work/$label.known" | uniq -c | awk '{ printf "%s%s x%d", sep, $2, $1; sep = ", " }')"
    if [ "$newcount" = 0 ]; then
        printf '%-8s%s\n' "$([ "$blite" = 0 ] && echo SAME || echo KNOWN)" "$label: $blite BLite warnings, expected 0${knownlist:+; all known candidates: $knownlist}"
    else
        verdict "FAIL    $label: $blite BLite warnings, expected 0; $newcount not registered${knownlist:+; known: $knownlist}"
    fi
}
ilc_warnings "$work/publish.log" > "$work/consumer.warnings"
count_warnings "$work/consumer.warnings" "Consumer publish" warning

# ── Consumer.Linq: the call-site promise ──────────────────────────────────────

t5=$(now)
fresh_native Consumer.Linq
if ! dotnet publish "$here/Consumer.Linq" "${publish_args[@]}" -o "$work/linq" > "$work/publish-linq.log" 2>&1; then
    verdict "ERROR   Consumer.Linq publish failed: $(grep -m1 -E 'error|ld: ' "$work/publish-linq.log" | sed 's/^ *//')"
else
    t6=$(now)
    ilc_warnings "$work/publish-linq.log" > "$work/linq.warnings"
    rel="tools/aot-smoke/Consumer.Linq/LinqCalls.cs"
    while IFS=: read -r lineno rest; do
        expected="$(echo "$rest" | sed -E 's/.*aot-expect:(.*)$/\1/' | tr -s ' ' '\n' | grep -E '^IL[0-9]+$' | sort -u | tr '\n' ' ')"
        call="$(echo "$rest" | sed -E 's/^ *//; s/ *\/\/ aot-expect:.*$//' | grep -oE '[A-Za-z]+\(' | grep -vE '^(Where|ToList|Subscribe|NullObserver|await)\(' | head -1 | tr -d '(')"
        actual="$(grep -F "$rel($lineno):" "$work/linq.warnings" | grep -oE 'warning IL[0-9]+' | cut -d' ' -f2 | sort -u | tr '\n' ' ')"
        if [ "$actual" = "$expected" ]; then
            verdict "SAME    call site LinqCalls.cs:$lineno $call: ${expected% } present"
        elif [ -z "$actual" ]; then
            verdict "FAIL    call site LinqCalls.cs:$lineno $call: expected ${expected% }, absent"
        else
            verdict "FAIL    call site LinqCalls.cs:$lineno $call: expected ${expected% }, got ${actual% }"
        fi
    done < <(grep -n '// aot-expect: IL' "$here/Consumer.Linq/LinqCalls.cs" | tr -d '\r')

    rollups="$(grep -E 'IL(2104|3053)' "$work/linq.warnings" | grep -E "Assembly 'BLite" || true)"
    [ -z "$rollups" ] && verdict "SAME    Consumer.Linq: no IL2104/IL3053 rollup for a BLite assembly" \
                      || verdict "FAIL    Consumer.Linq: rollup warnings for BLite: $(echo "$rollups" | grep -oE "Assembly '[^']+'" | sort -u | tr '\n' ' ')"
    unexpected="$(grep -F 'tools/aot-smoke/Consumer.Linq/' "$work/linq.warnings" | grep -v '/obj/' | grep -vF "$(grep -n '// aot-expect: IL' "$here/Consumer.Linq/LinqCalls.cs" | cut -d: -f1 | sed "s|^|$rel(|; s|$|):|")" || true)"
    [ -z "$unexpected" ] || verdict "FAIL    Consumer.Linq: warnings at unmarked lines of its own source: $(echo "$unexpected" | cut -d: -f1 | sort -u | tr '\n' ' ')"

    # What the annotated calls add inside BLite: warnings the Consumer publish does not have.
    normalise() { sed -E 's|tools/aot-smoke/Consumer(\.Linq)?/|tools/aot-smoke/<consumer>/|; s|AotSmoke\.Linq|AotSmoke|g' | sort -u; }
    normalise < "$work/consumer.warnings" > "$work/base.norm"
    grep -v -F "$rel(" "$work/linq.warnings" | normalise | comm -13 "$work/base.norm" - > "$work/linq-only.warnings"
    count_warnings "$work/linq-only.warnings" "Consumer.Linq additional" "warning|linq-warning"
fi

# ── Summary ───────────────────────────────────────────────────────────────────

echo "        timings: JIT build $(elapsed "$t0" "$t1") s, JIT run $(elapsed "$t1" "$t2") s, AOT publish $(elapsed "$t2" "$t3") s, AOT run $(elapsed "$t3" "$t4") s${t6:+, Consumer.Linq publish $(elapsed "$t5" "$t6") s}"
echo "        exit codes: JIT $jit_exit, AOT $aot_exit (1 = a section failed, see the dumps)"
if [ "$failures" = 0 ]; then
    echo "PASS    no new finding"
    exit 0
fi
echo "FAIL    $failures check(s) failed"
exit 1
