import os
import json


def get_config(mode):
    path = os.path.dirname(__file__)
    configs = [f for f in os.listdir(path) if f.endswith(".json")]
    for conf in configs:
        if mode == conf.lower().split(".")[0]:
            return json.load(open(os.path.join(path, conf), "r"))
    else:
        return False
