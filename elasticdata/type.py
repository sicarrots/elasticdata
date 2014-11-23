# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

from collections import MutableMapping
from abc import ABCMeta
from six import add_metaclass
from dateutil.parser import parse


class ValidationError(Exception):
    pass


class TypeMeta(ABCMeta):
    def __new__(mcs, name, bases, attrs):
        meta = {'scopes': dict(), 'timestamps': False}
        for base in bases:
            if hasattr(base, '_meta'):
                meta.update(base._meta)
        if 'Meta' in attrs:
            if hasattr(attrs['Meta'], 'scopes'):
                meta['scopes'].update(attrs['Meta'].scopes)
            if hasattr(attrs['Meta'], 'timestamps'):
                def coerce_timestamp(value):
                    return parse(value)
                meta['timestamps'] = attrs['Meta'].timestamps
                attrs['get_created_at'] = coerce_timestamp
                attrs['get_updated_at'] = coerce_timestamp
        attrs['_meta'] = meta
        return super(TypeMeta, mcs).__new__(mcs, name, bases, attrs)


@add_metaclass(TypeMeta)
class Type(MutableMapping):
    def __init__(self, data=None, scope=None):
        self._data = data or {}
        self._errors = {}
        self._scope = scope

    def to_storage(self):
        keys = self._get_keys()
        data = {}
        for key in keys:
            func = getattr(self, 'repr_' + key, lambda val: val)
            data[key] = func(self._data.get(key, None))
        return data

    def to_representation(self):
        keys = self._get_keys()
        data = {}
        for key in keys:
            func = getattr(self, 'get_' + key, lambda val: val)
            data[key] = func(self._data.get(key, None))
        return data

    def is_valid(self):
        self._errors = {}
        keys = self._get_keys()
        for key in keys:
            if hasattr(self, 'validate_' + key):
                try:
                    getattr(self, 'validate_' + key)(self._data.get(key, None))
                except ValidationError, e:
                    self.errors[key] = e
        if hasattr(self, 'validate'):
            try:
                getattr(self, 'validate')(self._data)
            except ValidationError, e:
                self._errors['_general'] = e
        if self._errors:
            return False
        return True

    @property
    def errors(self):
        return self._errors

    @property
    def type(self):
        return self.__class__.__name__.lower()

    @property
    def fields(self):
        if self._scope and self._scope in self._meta['scopes']:
            return self._meta['scopes'][self._scope]
        return None

    @classmethod
    def get_fields(cls, scope):
        if scope and scope in cls._meta['scopes']:
            return cls._meta['scopes'][scope]
        return None

    @classmethod
    def get_type(cls):
        return cls.__name__.lower()

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