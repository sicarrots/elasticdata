# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import six
from collections import OrderedDict
from importlib import import_module
from django.conf import settings
from elasticsearch import Elasticsearch, helpers, TransportError
from datetime import datetime

from .repository import BaseRepository

ADD, UPDATE, REMOVE = range(3)


def group(data, type_getter):
    """ Group items from iterable by type returned by type_getter on item
        :param data: iterable with items
        :param type_getter: callable for getting type from item
    """
    grouped = {}
    for item in data:
        key = type_getter(item)
        if key in grouped:
            grouped[key].append(item)
        else:
            grouped[key] = [item]
    return grouped


def without(keys, dct, move_up=None):
    """ Returns dictionary without listed keys

        Optionally can move up keys from nested dictionary to parent before removing key.
        :param keys: list of keys to remove
        :param dct: dictionary to perform removing
        :param move_up: definiton of keys which should be moved to parent
    """
    _dct = dct.copy()
    if move_up:
        for k, v in six.iteritems(move_up):
            for moved_key in v:
                _dct[moved_key] = _dct[k][moved_key]
    return {k: v for k, v in six.iteritems(_dct) if k not in keys}


class RepositoryError(Exception):
    def __init__(self, message, cause=None):
        #  Bu, exceptions chaining is avaliable only in py3.
        super(RepositoryError, self).__init__(message + ', caused by ' + repr(cause))
        self.cause = cause


class EntityNotFound(RepositoryError):
    pass


class PersistedEntity(object):
    def __init__(self, entity, state=ADD, index='default'):
        self._initial_value = None
        self._entity = entity
        if state == UPDATE:
            self.reset_state()
        self.state = state
        self._index = index
        self._registry = {}

    def get_stmt(self):
        if self.state == ADD:
            return self._add()
        elif self.state == UPDATE:
            return self._update()
        elif self.state == REMOVE:
            return self._remove()

    def reset_state(self):
        self._initial_value = self._entity.to_storage()
        if 'id' in self._initial_value:
            del self._initial_value['id']
        self.state = UPDATE

    def set_id(self, _id):
        self._entity['id'] = _id

    def _add(self):
        if self._entity._meta['timestamps']:
            now = datetime.now()
            self._entity['created_at'] = now
            self._entity['updated_at'] = now
        source = self._entity.to_storage()
        stmt = {
            '_op_type': 'create',
            '_index': self._index,
            '_type': self._entity.type,
        }
        if 'id' in source:
            stmt['_id'] = source['id']
            del source['id']
        if '_parent' in self._entity:
            stmt['_parent'] = self._entity['_parent']
        stmt['_source'] = source
        return stmt

    def _update(self):
        if 'id' not in self._entity:
            return None
        if self._entity._meta['timestamps']:
            self._entity['updated_at'] = datetime.now()
        diff = self._update_diff()
        if not diff:
            return None
        return {
            '_op_type': 'update',
            '_index': self._index,
            '_type': self._entity.type,
            '_id': self._entity['id'],
            'doc': diff
        }

    def _remove(self):
        if 'id' not in self._entity:
            return None
        return {
            '_op_type': 'delete',
            '_index': self._index,
            '_type': self._entity.type,
            '_id': self._entity['id'],
        }

    def _update_diff(self):
        current_state = self._entity.to_storage()
        if 'id' in current_state:
            del current_state['id']
        diff = {}
        for k, v in six.iteritems(current_state):
            if (k not in self._initial_value) or (k in self._initial_value and v != self._initial_value[k]):
                diff[k] = v
        for k in set(self._initial_value.keys()) - set(current_state.keys()):
            diff[k] = None
        return diff


class EntityManager(object):
    def __init__(self, index='default', es_settings=None):
        if es_settings:
            self.es = Elasticsearch(**es_settings)
        else:
            self.es = Elasticsearch()
        self._index = index
        self._registry = {}

    def persist(self, entity):
        if not hasattr(entity, 'to_storage') or not hasattr(entity, '__getitem__') or not hasattr(entity, 'type'):
            raise TypeError('entity object must have to_storage, type and behave like a dict methods')
        self._persist(entity, state=ADD)

    def remove(self, entity):
        self._persist(entity, state=REMOVE)

    def flush(self):
        actions = OrderedDict()
        for pe in self._registry.itervalues():
            stmt = pe.get_stmt()
            if stmt:
                actions[pe] = stmt
        self._execute_callbacks(actions, 'pre')
        blk = [result for result in helpers.streaming_bulk(self.es, actions.itervalues())]  # TODO: exceptions?
        for i, pe in enumerate(actions.iterkeys()):
            if 'create' in blk[i][1]:
                pe.set_id(blk[i][1]['create']['_id'])
            pe.reset_state()
        self._execute_callbacks(actions, 'post')

    def find(self, _id, _type, scope=None):
        kwargs = {'id': _id, 'index': self._index, 'doc_type': _type.get_type()}
        if scope:
            kwargs['_source'] = _type.get_fields(scope)
        try:
            _data = self.es.get(**kwargs)
        except TransportError as e:  # TODO: the might be other errors like server unavaliable
            raise EntityNotFound('Entity {type} {_id} not found.'.format(type=_type.get_type(), _id=_id), e)
        source = _data['_source']
        source['id'] = _data['_id']
        entity = _type(source, scope)
        self._persist(entity, state=UPDATE)
        return entity

    def find_many(self, _ids, _type, scope=None):
        kwargs = {'body': {'ids': _ids}, 'index': self._index}
        if scope:
            kwargs['_source'] = _type.get_fields(scope)
        try:
            _data = self.es.mget(**kwargs)
        except TransportError as e:  # TODO: the might be other errors like server unavaliable
            raise EntityNotFound('Entity {type} {_id} not found.'.format(
                type=_type.get_type(), _id=', '.join(_ids)), e)
        entities = []
        for _entity in _data['docs']:
            source = _entity['_source']
            source['id'] = _entity['_id']
            entity = _type(source, scope)
            self._persist(entity, state=UPDATE)
            entities.append(entity)
        return entities

    def query(self, query, _type, scope=None):
        kwargs = {}
        if scope:
            kwargs['_source'] = _type.get_fields(scope)
        try:
            data = self.es.search(index=self._index, doc_type=_type.get_type(), body=query.update(kwargs))
        except TransportError as e:
            raise RepositoryError('Transport returned error', cause=e)
        entities = []
        for record in data['hits']['hits']:
            source = record['_source']
            source['id'] = record['_id']
            source['_score'] = record['_score']
            entity = _type(source, scope)
            self._persist(entity, state=UPDATE)
            entities.append(entity)
        return entities, without(['hits'], data, move_up={'hits': ['max_score', 'total']})

    def query_one(self, query, _type, scope=None):
        entities, meta = self.query(query, _type, scope)
        if len(entities) == 1:
            return entities[0]
        raise RepositoryError('Expected one result, found {num}'.format(num=len(entities)))

    def clear(self):
        self._registry = {}

    def get_repository(self, repository):
        app, repository_class_name = repository.split(':')
        if app not in settings.INSTALLED_APPS:
            app = filter(lambda _app: _app.endswith(app), settings.INSTALLED_APPS)
            if not app:
                raise RepositoryError('Given application {app} are not in INSTALLED_APPS'.format(app=app))
        try:
            module = import_module(app + '.' + 'repositories')
        except ImportError:
            raise RepositoryError('Given application {app} has no repositories'.format(app=app))
        if not hasattr(module, repository_class_name):
            raise RepositoryError(
                'Given repository {repository_class_name} does not exists in application {app}'.format(
                    repository_class_name=repository_class_name, app=app
                ))
        repository_class = getattr(module, repository_class_name)
        if not isinstance(repository_class, BaseRepository):
            raise RepositoryError('Custom repository must be subclass of BaseRepository')
        return repository_class(self)

    def get_client(self):
        return self.es

    def _persist(self, entity, state):
        if id(entity) in self._registry:
            self._registry[id(entity)].state = state
        else:
            self._registry[id(entity)] = PersistedEntity(entity, state=state, index=self._index)

    def _execute_callbacks(self, actions, type):
        for persisted_entity, stmt in six.iteritems(actions):
            action = stmt['_op_type']
            callback_func_name = type + '_' + action
            if hasattr(persisted_entity._entity, callback_func_name):
                getattr(persisted_entity._entity, callback_func_name)(self)

