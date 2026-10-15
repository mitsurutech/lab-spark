#!/usr/bin/env bash
# What the README promises, checked from inside the lab's own network: run by check.sh.
set -euo pipefail
: "${NET:?run through test/check.sh}"
probe() { docker run --rm --network "$NET" lab-check-probe bash -c "$1"; }
echo "- the CSV is loaded: SQL counts its 24 orders"
probe '
set -eu
out=$(curl -fs -d "SELECT count(*) AS n FROM orders" http://spark:8000/sql)
echo "$out" | grep -q "| 24|" || { echo "no 24 in: $out" >&2; exit 1; }
echo "- a DataFrame filter keeps the UK rows only (7 of them)"
out=$(curl -fs -d "orders.filter(orders.country == \"UK\").select(\"customer\")" http://spark:8000/df)
test "$(echo "$out" | grep -c "^| *[A-Z]")" = 7 || { echo "not seven UK rows: $out" >&2; exit 1; }
echo "- group and count: Japan has 6 orders"
out=$(curl -fs -d "orders.groupBy(\"country\").count()" http://spark:8000/df)
echo "$out" | grep -Eq "Japan *\| *6" || { echo "Japan is not 6: $out" >&2; exit 1; }
echo "- revenue per country by SQL, largest first, Japan on top, 717.5"
out=$(curl -fs -d "SELECT country, round(sum(quantity * unit_price), 2) AS revenue FROM orders GROUP BY country ORDER BY revenue DESC" http://spark:8000/sql)
echo "$out" | sed -n 4p | grep -q "Japan.*717.5" || { echo "Japan is not on top: $out" >&2; exit 1; }
echo "- a wrong query is told, not a crash"
code=$(curl -s -o /dev/null -w "%{http_code}" -d "SELECT nope FROM orders" http://spark:8000/sql)
[ "$code" = 400 ] || { echo "bad query answered $code" >&2; exit 1; }
echo "- the Spark UI shows the jobs those queries made"
curl -fs http://spark:4040/api/v1/applications | grep -q "lab-spark"
id=$(curl -fs http://spark:4040/api/v1/applications | python3 -c "import json,sys; print(json.load(sys.stdin)[0][\"id\"])")
n=$(curl -fs http://spark:4040/api/v1/applications/$id/jobs | python3 -c "import json,sys; print(len(json.load(sys.stdin)))")
[ "$n" -ge 3 ] || { echo "only $n jobs in the UI" >&2; exit 1; }
curl -fs -o /dev/null http://spark:4040/jobs/
echo "- no way out to the internet"
if curl -s -m 4 -o /dev/null https://example.com; then echo "the lab reached the internet" >&2; exit 1; fi
'
echo "  ok"
