# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.conf import settings

from .type import Type, ValidationError
from .manager import EntityManager, RepositoryError, EntityNotFound


def get_entity_manager(index=None, es_settings=None):
    if not index:
        index = getattr(settings, 'ELASTICSEARCH_INDEX', 'default')
    if not es_settings:
        es_settings = getattr(settings, 'ELASTICSEARCH_CONFIG', None)
    return EntityManager(index=index, es_settings=es_settings)