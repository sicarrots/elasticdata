# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.conf import settings
from elasticsearch import Elasticsearch

from .type import Type, ValidationError
from .manager import EntityManager, RepositoryError, EntityNotFound


def get_entity_manager(index=None, es_settings=None):
    return EntityManager(index=get_index(index), es_settings=get_es_settings(es_settings))


def get_client(es_settings=None):
    es_settings = get_es_settings(es_settings)
    if es_settings:
        return Elasticsearch(**es_settings)
    else:
        return Elasticsearch()


def get_index(index=None):
    if not index:
        return getattr(settings, 'ELASTICSEARCH_INDEX', 'default')
    return index


def get_es_settings(es_settings):
    if not es_settings:
        return getattr(settings, 'ELASTICSEARCH_CONFIG', None)
    return es_settings