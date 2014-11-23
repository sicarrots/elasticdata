# -*- coding: utf-8 -*-
from __future__ import unicode_literals


class BaseRepository(object):
    def __init__(self, entity_manager):
        self.em = entity_manager