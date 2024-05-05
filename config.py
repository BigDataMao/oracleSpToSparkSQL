# -*- coding: utf-8 -*-

import json


def load_config(filename):
    with open(filename, 'r') as f:
        config = json.load(f)
    return config


class Config:
    def __init__(self, filename):
        self.filename = filename
        self.data = load_config(filename)

    def get(self, key):
        return self.data.get(key)

    def get_logger(self):
        return self.data.get('logger')
