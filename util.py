# -*- coding: utf-8 -*-
"""
Created on Wed Nov 20 10:29:40 2019

@author: bswxj01
"""

def safeget (dct, *keys):
    for key in keys:
        try:
            dct = dct[key]
        except KeyError:
            return None
    return dct