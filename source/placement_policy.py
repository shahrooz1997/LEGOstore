class PlacementPolicy:
    def __init__(policy):
        properties = json.load(open('config.json'))["placementPolicy"]
        if policy == "AllInOne":
            return AllInOnePolicy()

        if policy == "MajorityinOne":
            return MajorityInOnePolicy()

        if policy == "UniformDistribution":
            return UniformDistributionPolicy()

        raise Exception("No placement policy found")
