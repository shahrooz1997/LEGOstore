#~/usr/bin/env bash

output=$(gcloud compute instances list --format="value(name)" --filter="name ~ ^data-server.*" --quiet)

len=$(echo "$output" |  awk 'END {print NR}')

output=$(gcloud compute instances list --format="value(name)" --filter="name ~ ^controller.*" --quiet)

len2=$(echo "$output" | awk 'END {print NR}')

echo "There are $len Data servers and $len2 controllers running"

exit 0
