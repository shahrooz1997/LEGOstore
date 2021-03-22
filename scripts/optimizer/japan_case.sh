

python3 placement.py -f tests/inputtests/dc.json -i skewed_japan.json -o japan_case/skewed_japan_res.json -H min_latency
python3 placement.py -f tests/inputtests/dc_5.json -i skewed_japan_closest.json -o japan_case/skewed_japan_closest_res.json -H min_latency -t abd -b
python3 placement.py -f tests/inputtests/dc.json -i skewed_japan.json -o japan_case/skewed_japan_res_cost_h.json -H min_cost
python3 placement.py -f tests/inputtests/dc_5.json -i skewed_japan_closest.json -o japan_case/skewed_japan_closest_res_cost_h.json -H min_cost -t abd -b
