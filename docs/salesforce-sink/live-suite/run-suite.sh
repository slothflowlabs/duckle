#!/usr/bin/env bash
# Live Salesforce connector test suite (#166 stage 2 + Tier-1 regression).
# Runs the full record lifecycle against a REAL Salesforce org via the
# headless runner - see README.md for org prerequisites and env vars.
#
#   SF_INSTANCE_URL=... SF_CLIENT_ID=... SF_CLIENT_SECRET=... bash run-suite.sh
#
# Part 1 - lifecycle, every step through the saved connection (connections/
# sf-live.json - a connectionRef-resolved connection whose fields are
# ${ENV:} placeholders, so no credential ever lives in a file):
#   s1  INSERT    csv -> snk.salesforce insert            (SUITE-101/102)
#   s2  RETRIEVE  src.salesforce query -> csv
#   s3  UPDATE    retrieved rows, idField REMAPPED (RecId -> Id)
#   s4  UPSERT    by External_ID__c: overwrites SUITE-101, creates SUITE-201
#   s5  RETRIEVE  re-read; assert update + overwrite + new row
#   s6  DELETE    retrieved Ids; assert org clean
# Part 2 - auth matrix + failure modes:
#   t4  inline ${ENV:} bearer upsert (token minted below; back-compat path)
#   t6  wrong-kind connectionRef (postgres) -> clear error
#   t7  missing connection id              -> clear error
#
# Suite records carry External_ID__c = SUITE-* and s6/final cleanup delete
# them, so repeat runs start clean.
set -u
cd "$(dirname "$0")"
RUNNER=${DUCKLE_RUNNER:-duckle-runner}
WS=$(pwd)

: "${SF_INSTANCE_URL:?set SF_INSTANCE_URL (e.g. https://acme.my.salesforce.com)}"
: "${SF_CLIENT_ID:?set SF_CLIENT_ID (connected-app consumer key)}"
: "${SF_CLIENT_SECRET:?set SF_CLIENT_SECRET}"

pass=0; fail=0; results=()

check() { # $1 name, $2 expect ("ok" or grep pattern for the error), $3 output
  local name=$1 expect=$2 out=$3
  if [ "$expect" = "ok" ]; then
    if grep -q '^status   : ok' <<<"$out"; then results+=("PASS  $name"); ((pass++)); return 0; fi
  else
    if grep -qi "$expect" <<<"$out" && ! grep -q '^status   : ok' <<<"$out"; then results+=("PASS  $name"); ((pass++)); return 0; fi
  fi
  results+=("FAIL  $name"); ((fail++)); echo "---- $name output ----"; echo "$out" | tail -8
}

assert_file() { # $1 name, $2 file, $3 grep pattern
  if grep -q "$3" "$2" 2>/dev/null; then results+=("PASS  $1"); ((pass++));
  else results+=("FAIL  $1"); ((fail++)); fi
}

run_pipe() { "$RUNNER" --pipeline "$1" --workspace "$WS" 2>&1; }

# SOQL right after a write can briefly return stale rows; retry the read.
retry_read() { # $1 pipeline, $2 out-file, $3 pattern that must appear
  for attempt in 1 2 3; do
    rm -f "$2"; run_pipe "$1" >/dev/null 2>&1
    grep -q "$3" "$2" 2>/dev/null && return 0
    sleep 5
  done
  return 1
}

# ---- Part 1: the record lifecycle -----------------------------------------

rm -rf out/sf-results
out=$(run_pipe s1_insert.json)
check "s1 insert csv -> salesforce (saved connection)" ok "$out"
# #166 resultsPath: s1 also writes Data-Loader-style per-record result files,
# stamped {object}_{operation}_{utc} so repeat runs accumulate.
s_file=$(ls out/sf-results/Account_insert_*_success.csv 2>/dev/null | head -1)
s_rows=$(( $( [ -n "$s_file" ] && wc -l < "$s_file" || echo 0 ) - 1 ))
if [ "$s_rows" -eq 2 ] && grep -q 'sf__Id' "$s_file" 2>/dev/null; then
  results+=("PASS  s1b stamped success csv: 2 rows + sf__Id column"); ((pass++))
else
  results+=("FAIL  s1b stamped success csv: 2 rows + sf__Id column"); ((fail++))
fi
e_file=$(ls out/sf-results/Account_insert_*_error.csv 2>/dev/null | head -1)
e_lines=$(( $( [ -n "$e_file" ] && wc -l < "$e_file" || echo 0 ) ))
if [ "$e_lines" -eq 1 ]; then
  results+=("PASS  s1c stamped error csv written header-only"); ((pass++))
else
  results+=("FAIL  s1c stamped error csv written header-only"); ((fail++))
fi

if retry_read s2_retrieve.json out/retrieved.csv 'SUITE-101'; then
  results+=("PASS  s2 retrieve inserted records"); ((pass++))
else
  results+=("FAIL  s2 retrieve inserted records"); ((fail++))
fi
assert_file "s2b both inserted rows present" out/retrieved.csv 'Suite Insert B'

out=$(run_pipe s3_update.json)
check "s3 update retrieved records (remapped idField RecId)" ok "$out"
if retry_read s5_retrieve2.json out/retrieved2.csv 'Updated Suite Insert B'; then
  results+=("PASS  s3b update visible on re-read"); ((pass++))
else
  results+=("FAIL  s3b update visible on re-read"); ((fail++))
fi

out=$(run_pipe s4_upsert.json)
check "s4 upsert by External_ID__c" ok "$out"

if retry_read s5_retrieve2.json out/retrieved2.csv 'Suite Upsert New'; then
  results+=("PASS  s5 retrieve after upsert (new row present)"); ((pass++))
else
  results+=("FAIL  s5 retrieve after upsert (new row present)"); ((fail++))
fi
assert_file "s5b upsert overwrote SUITE-101"    out/retrieved2.csv 'Suite Upsert Overwrite A'
assert_file "s5c s3 update intact on SUITE-102" out/retrieved2.csv 'Updated Suite Insert B'

out=$(run_pipe s6_delete.json)
check "s6 delete retrieved records" ok "$out"
# Emptiness check: s5's source declares a schema, so a 0-record query flows
# through as a typed empty relation (#170) and the csv lands header-only.
clean=""
for attempt in 1 2 3; do
  sleep 5; rm -f out/retrieved2.csv
  out=$(run_pipe s5_retrieve2.json)
  if [ -f out/retrieved2.csv ]; then lines=$(wc -l < out/retrieved2.csv); else lines=99; fi
  [ "$lines" -le 1 ] && { clean=yes; break; }
done
if [ -n "$clean" ]; then results+=("PASS  s6b org clean after delete"); ((pass++));
else results+=("FAIL  s6b org clean after delete"); ((fail++)); fi

# ---- Part 2: auth matrix + failure modes -----------------------------------

# Mint a bearer token from the same client-credentials app so the inline
# ${ENV:SF_TOKEN} back-compat path is exercised too.
tok_resp=$(curl -s -X POST "${SF_INSTANCE_URL%/}/services/oauth2/token" \
  -d grant_type=client_credentials -d "client_id=$SF_CLIENT_ID" -d "client_secret=$SF_CLIENT_SECRET")
SF_TOKEN=$(sed -n 's/.*"access_token" *: *"\([^"]*\)".*/\1/p' <<<"$tok_resp")
export SF_TOKEN
if [ -n "$SF_TOKEN" ]; then
  out=$(run_pipe t4_bearer_inline.json)
  check "t4 inline \${ENV:} bearer back-compat" ok "$out"
else
  results+=("FAIL  t4 inline bearer (token mint failed: ${tok_resp:0:120})"); ((fail++))
fi

out=$(run_pipe t6_wrongkind.json)
check "t6 wrong-kind connectionRef errors" "expected a Salesforce connection" "$out"

out=$(run_pipe t7_missingref.json)
check "t7 missing connection id errors" "not found" "$out"

# Final cleanup: t4 upserted SUITE-301 after s6's delete; remove whatever
# SUITE-* remains so repeat runs start clean.
out=$(run_pipe s6_delete.json)
check "final cleanup delete" ok "$out"

echo; echo "==== suite results ===="
printf '%s\n' "${results[@]}"
echo "==== $pass passed, $fail failed ===="
exit $((fail > 0))
