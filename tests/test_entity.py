# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from unittest import TestCase

from elasticdata import Type, ValidationError


class TestType(Type):
    class Meta:
        scopes = {
            'small': ('foo', 'bar'),
            'large': ('foo', 'bar', 'baz')
        }

    def repr_foo(self, value, context):
        return 'repr({value})'.format(value=value)

    def get_foo(self, value, context):
        return 'get({value})'.format(value=value)

    def validate_bar(self, value, context):
        if value is None:
            raise ValidationError('bar cannot be None')

    def validate(self, attributes, context):
        if attributes.get('foo', None) != attributes.get('bar', None):
            raise ValidationError('foo must be bar')


class ExtendedTestType(TestType):
    class Meta:
        scopes = {
            'medium': ('foo', 'baz')
        }
        timestamps = True


class ContextTestType(TestType):
    def get_foo(self, value, context):
        return context

    def repr_foo(self, value, context):
        return context

    def validate_foo(self, value, context):
        if not context:
            raise ValidationError()

    def validate(self, attributes, context):
        if not context:
            raise ValidationError()



class TypeTestCase(TestCase):

    def setUp(self):
        self.DATA = {'foo': 'bar', 'bar': 'baz', 'baz': 'foo'}

    def test_get_set(self):
        te = TestType(self.DATA)
        self.assertEqual(te['foo'], 'bar')
        self.assertRaises(KeyError, lambda key: te[key], 'unknown')
        te['foo'] = 'foo'
        self.assertEqual(te['foo'], 'foo')

    def test_del(self):
        te = TestType(self.DATA)
        self.assertEqual(te['bar'], 'baz')
        del te['bar']
        self.assertRaises(KeyError, lambda key: te[key], 'bar')

    def test_to_representation(self):
        te = TestType(self.DATA)
        self.assertDictEqual(te.to_representation(), {'foo': 'get(bar)', 'bar': 'baz', 'baz': 'foo'})
        te = TestType({'foo': 'bar', '_attr': 'vaL'})
        self.assertDictEqual(te.to_representation(), {'foo': 'get(bar)'})

    def test_to_storage(self):
        te = TestType(self.DATA)
        self.assertDictEqual(te.to_storage(), {'foo': 'repr(bar)', 'bar': 'baz', 'baz': 'foo'})
        te = TestType({'foo': 'bar', '_attr': 'vaL'})
        self.assertDictEqual(te.to_storage(), {'foo': 'repr(bar)'})

    def test_validation(self):
        te = TestType(self.DATA)
        self.assertFalse(te.is_valid())
        self.assertEqual(len(te.errors), 1)
        te['bar'] = 'bar'
        self.assertTrue(te.is_valid())
        del te['bar']
        self.assertFalse(te.is_valid())
        self.assertEqual(len(te.errors), 1)

    def test_type(self):
        te = TestType()
        self.assertEqual(te.type, 'testtype')
        ete = ExtendedTestType()
        self.assertEqual(ete.type, 'extendedtesttype')

    def test_scope(self):
        te_small = TestType(self.DATA, scope='small')
        te_large = TestType(self.DATA, scope='large')
        self.assertEqual(te_small.fields, ('foo', 'bar'))
        self.assertEqual(te_large.fields, ('foo', 'bar', 'baz'))
        self.assertEqual(TestType.get_fields('small'), ('foo', 'bar'))
        self.assertEqual(TestType.get_fields('large'), ('foo', 'bar', 'baz'))
        self.assertDictEqual(te_small.to_representation(), {'foo': 'get(bar)', 'bar': 'baz'})
        self.assertDictEqual(te_small.to_storage(), {'foo': 'repr(bar)', 'bar': 'baz'})
        self.assertDictEqual(te_large.to_representation(), {'foo': 'get(bar)', 'bar': 'baz', 'baz': 'foo'})
        self.assertDictEqual(te_large.to_storage(), {'foo': 'repr(bar)', 'bar': 'baz', 'baz': 'foo'})

    def test_scope_inheritance(self):
        te_small = ExtendedTestType(self.DATA, scope='small')
        te_medium = ExtendedTestType(self.DATA, scope='medium')
        te_large = ExtendedTestType(self.DATA, scope='large')
        self.assertEqual(te_small.fields, ('foo', 'bar'))
        self.assertEqual(te_medium.fields, ('foo', 'baz'))
        self.assertEqual(te_large.fields, ('foo', 'bar', 'baz'))
        self.assertDictEqual(te_small.to_representation(), {'foo': 'get(bar)', 'bar': 'baz'})
        self.assertDictEqual(te_small.to_storage(), {'foo': 'repr(bar)', 'bar': 'baz'})
        self.assertDictEqual(te_medium.to_representation(), {'foo': 'get(bar)', 'baz': 'foo'})
        self.assertDictEqual(te_medium.to_storage(), {'foo': 'repr(bar)', 'baz': 'foo'})
        self.assertDictEqual(te_large.to_representation(), {'foo': 'get(bar)', 'bar': 'baz', 'baz': 'foo'})
        self.assertDictEqual(te_large.to_storage(), {'foo': 'repr(bar)', 'bar': 'baz', 'baz': 'foo'})

    def test_timestamps(self):
        te = TestType()
        self.assertFalse(te._meta['timestamps'])
        te2 = ExtendedTestType()
        self.assertTrue(te2._meta['timestamps'])

    def test_passing_context(self):
        te = ContextTestType({'foo': 'bar'})
        self.assertDictEqual({'foo': 'value from context'}, te.to_representation('value from context'))
        self.assertDictEqual({'foo': 'context'}, te.to_storage('context'))
        self.assertFalse(te.is_valid(False))
        self.assertEqual(len(te.errors), 2)
        self.assertTrue(te.is_valid(True))
