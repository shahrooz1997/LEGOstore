ABD = 'abd'
CAS = 'cas'
CAS_K_1 = 'cas_k_1'
REP = 'replication'
EC = 'ec'
BRUTE_FORCE = 'brute_force'
MIN_COST = 'min_cost'
MIN_LATENCY = 'min_latency'
BASELINE0 = 'baseline0'
BASELINE1 = 'baseline1'

GROUP = "Group"
DATACENTER = "DataCenter"

GEN_ABD = 'gen_abd_params'
GEN_CAS = 'gen_cas_params'

PLACEMENT_ABD = 'PlacementAbd'
PLACEMENT_CAS = 'PlacementCas'
PLACEMENT_REP = 'PlacementRep'
PLACEMENT_EC = 'PlacementEC'
PLACEMENT_BASE = 'PlacementBase'

GEN_PARAM_FUNC = {
    ABD: GEN_ABD,
    CAS: GEN_CAS,
    REP: GEN_CAS
}

PLACEMENT_CLASS_MAPPER = {
    ABD: PLACEMENT_ABD,
    CAS: PLACEMENT_CAS,
    REP: PLACEMENT_REP,
    EC:  PLACEMENT_EC
}

FUNC_HEURISTIC_MAP = {
    ABD: {
        MIN_COST: MIN_COST+'_'+ABD,
        MIN_LATENCY: MIN_LATENCY+'_'+ABD,
        BRUTE_FORCE: BRUTE_FORCE+'_'+ABD,
        BASELINE0: BASELINE0+'_'+ABD,
        BASELINE1: BASELINE1+'_'+ABD
    },

    CAS: {
        MIN_COST: MIN_COST+'_'+CAS,
        MIN_LATENCY: MIN_LATENCY+'_'+CAS,
        BRUTE_FORCE: BRUTE_FORCE+'_'+CAS,
        BASELINE0: BASELINE0+'_'+CAS,
        BASELINE1: BASELINE1+'_'+CAS
    },

    REP: {
        MIN_COST: MIN_COST+'_'+CAS,
        MIN_LATENCY: MIN_LATENCY+'_'+CAS,
        BRUTE_FORCE: BRUTE_FORCE+'_'+CAS,
        BASELINE0: "baseline0"+'_'+CAS,
        BASELINE1: BASELINE1+'_'+CAS
    }
}
