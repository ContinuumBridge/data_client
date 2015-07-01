#!/usr/bin/env python
# check_config.py
# Copyright (C) ContinuumBridge Limited, 2015 - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited
# Proprietary and confidential
# Written by Peter Claydon
#
"""
Checks the legality of config file
"""
import json
import sys

if len(sys.argv) < 2:
    print("Improper number of arguments. Requires a file name")
    exit(1)
CONFIG_FILE = sys.argv[1]

config        = {}

try:
    oldconfig = config
    with open(CONFIG_FILE, 'r') as f:
        newConfig = json.load(f)
        print( "Read config file")
        config.update(newConfig)
    for c in config:
        if c.lower in ("true", "t", "1"):
            config[c] = True
        elif c.lower in ("false", "f", "0"):
            config[c] = False
    print("Config file OK:")
    print(json.dumps(config, indent=4))
except Exception as ex:
    print("Problem reading config file, type: %s, exception: %s", str(type(ex)), str(ex.args))

