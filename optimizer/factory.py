import sys
import constants.opt_consts as CONSTS
from cls import PlacementBase, PlacementAbd, PlacementCas, Group, DataCenter

def obj_factory(cls, **kwargs):
    """ Factory method to create objects
    """
    obj = eval(cls)(**kwargs)
    return obj

def set_attr_from_json(json_dict, obj):
    for key, val in json_dict.items():
        obj.__dict__[key.lower()] = val

def json_to_obj(json_dict, cls, **kwargs):
    obj = obj_factory(cls, **kwargs)
    set_attr_from_json(json_dict, obj)
    return obj
