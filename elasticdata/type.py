# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

import copy
from collections import MutableMapping
from abc import ABCMeta
from six import add_metaclass
from inflection import underscore


class ValidationError(Exception):
    pass


class TypeMeta(ABCMeta):
    def __new__(mcs, name, bases, attrs):
        meta = {
            'scopes': dict(),
            'timestamps': False,
        }
        for base in bases:
            if hasattr(base, '_meta'):
                meta.update(base._meta)
                if hasattr(base._meta, 'timestamps'):
                    meta['timestamps'] = base._meta.timestamps
        if 'Meta' in attrs:
            if hasattr(attrs['Meta'], 'scopes'):
                meta['scopes'].update(attrs['Meta'].scopes)
            if hasattr(attrs['Meta'], 'timestamps'):
                meta['timestamps'] = attrs['Meta'].timestamps
        attrs['_meta'] = copy.deepcopy(meta)
        return super(TypeMeta, mcs).__new__(mcs, name, bases, attrs)


@add_metaclass(TypeMeta)
class Type(MutableMapping):
    def __init__(self, data=None, scope=None, highlight=None):
        self._data = data or {}
        self._errors = {}
        self._scope = scope
        self._highlight = highlight

    def to_storage(self, *args, **kwargs):
        keys = self._get_keys()
        data = {}
        for key in keys:
            value = self._data.get(key, None)
            func = getattr(self, 'repr_' + key, None)
            if func is not None:
                data[key] = func(value, *args, **kwargs)
            else:
                data[key] = value
        return data

    def to_representation(self, *args, **kwargs):
        keys = self._get_keys()
        data = {}
        for key in keys:
            func = getattr(self, 'get_' + key, None)
            value = self._data.get(key, None)
            if func is not None:
                data[key] = func(value, *args, **kwargs)
            else:
                data[key] = value
        return data

    def is_valid(self, *args, **kwargs):
        self._errors = {}
        keys = self._get_keys()
        for key in keys:
            if hasattr(self, 'validate_' + key):
                try:
                    getattr(self, 'validate_' + key)(self._data.get(key, None), *args, **kwargs)
                except ValidationError as e:
                    self.errors[key] = e
        if hasattr(self, 'validate'):
            try:
                getattr(self, 'validate')(self._data, *args, **kwargs)
            except ValidationError as e:
                self._errors['_general'] = e
        if self._errors:
            return False
        return True

    @property
    def errors(self):
        return self._errors

    @property
    def type(self):
        return underscore(self.__class__.__name__).lower()

    @property
    def fields(self):
        if self._scope and self._scope in self._meta['scopes']:
            return self._meta['scopes'][self._scope]
        return None

    @property
    def scope(self):
        return self._scope

    @property
    def highlight(self):
        return self._highlight

    @classmethod
    def get_fields(cls, scope):
        if scope and scope in cls._meta['scopes']:
            return filter(lambda f: f != 'id', cls._meta['scopes'][scope])
        return None

    @classmethod
    def get_type(cls):
        return underscore(cls.__name__).lower()

    def __getitem__(self, item):
        return self._data[item]

    def __setitem__(self, item, value):
        self._data[item] = value

    def __delitem__(self, item):
        del self._data[item]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def _get_keys(self):
        if self._scope and self._scope in self._meta['scopes']:
            return self._meta['scopes'][self._scope]
        return filter(lambda key: not key.startswith('_'), self._data.keys())


class TimestampedType(Type):
    class Meta:
        timestamps = True