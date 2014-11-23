# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import random
import string
from unittest import TestCase
from elasticsearch import Elasticsearch

from elasticdata.manager import without, PersistedEntity, EntityManager, UPDATE, REMOVE, ADD, RepositoryError,\
    EntityNotFound
from elasticdata import Type


class TestType(Type):
    class Meta:
        scopes = {
            'all': ('foo', 'bar'),
            'small': ('foo', )
        }


class TimestampsType(Type):
    class Meta:
        timestamps = True


class HelpersTestCase(TestCase):
    def test_without(self):
        self.assertDictEqual({'key1': 1, 'key2': 2}, without(['key3'], {'key1': 1, 'key2': 2, 'key3': 3}))
        self.assertDictEqual({'key1': 1, 'key2': 2, 'key4': 4},
                             without(['key3'],
                                     {'key1': 1, 'key2': 2, 'key3': {'key4': 4}},
                                     move_up={'key3': ['key4']}))


class PersistedEntityTestCase(TestCase):
    def test_new_entity(self):
        e = TestType({'foo': 'bar'})
        pe = PersistedEntity(e)
        self.assertDictEqual(pe.get_stmt(), {'_index': 'default', '_source': {'foo': 'bar'}, '_type': 'testtype'})
        e = TestType({'foo': 'bar', 'id': 1})
        pe = PersistedEntity(e)
        self.assertDictEqual(pe.get_stmt(),
                             {'_index': 'default', '_source': {'foo': 'bar'}, '_type': 'testtype', '_id': 1})
        e = TestType({'foo': 'bar', 'id': 1, '_parent': '2'})
        pe = PersistedEntity(e)
        self.assertDictEqual(pe.get_stmt(),
                             {'_index': 'default', '_source': {'foo': 'bar'},
                              '_type': 'testtype', '_id': 1, '_parent': '2'})

    def test_update_entity(self):
        e = TestType({'foo': 'bar', 'id': 1})
        pe = PersistedEntity(e, state=UPDATE)
        self.assertIsNone(pe.get_stmt())
        e['bar'] = 'baz'
        self.assertDictEqual(pe.get_stmt(), {
            '_id': 1,
            '_index': 'default',
            '_op_type': 'update',
            '_type': 'testtype',
            'doc': {'bar': 'baz'}
        })
        pe.reset_state()
        self.assertIsNone(pe.get_stmt())
        e['foo'] = 'baz'
        self.assertDictEqual(pe.get_stmt(), {
            '_id': 1,
            '_index': 'default',
            '_op_type': 'update',
            '_type': 'testtype',
            'doc': {'foo': 'baz'}
        })
        pe.reset_state()
        self.assertIsNone(pe.get_stmt())
        del e['bar']
        self.assertDictEqual(pe.get_stmt(), {
            '_id': 1,
            '_index': 'default',
            '_op_type': 'update',
            '_type': 'testtype',
            'doc': {'bar': None}
        })
        pe.reset_state()
        self.assertIsNone(pe.get_stmt())
        e['foo'] = 'baz'
        self.assertIsNone(pe.get_stmt())

    def test_delete_entity(self):
        e = TestType({'foo': 'bar'})
        pe = PersistedEntity(e, state=REMOVE)
        self.assertIsNone(pe.get_stmt())
        e = TestType({'foo': 'bar', 'id': '1'})
        pe = PersistedEntity(e, state=REMOVE)
        self.assertDictEqual(pe.get_stmt(), {
            '_id': '1',
            '_index': 'default',
            '_op_type': 'delete',
            '_type': 'testtype'
        })


class EntityManagerTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        super(EntityManagerTestCase, cls).setUpClass()
        cls._index = ''.join(random.choice(string.ascii_lowercase) for _ in range(10))

    def tearDown(self):
        es = Elasticsearch()
        es.indices.delete(index=self._index, ignore=[404])

    @property
    def em(self):
        return EntityManager(index=self._index)

    def test_persist(self):
        em = self.em
        e = TestType({'foo': 'bar'})
        em.persist(e)
        self.assertEqual(len(em._registry), 1)
        e2 = TestType({'bar': 'baz'})
        em.persist(e2)
        self.assertEqual(len(em._registry), 2)
        em.persist(e)
        self.assertEqual(len(em._registry), 2)
        self.assertRaises(TypeError, em.persist, dict())

    def test_remove(self):
        em = self.em
        e = TestType({'foo': 'bar'})
        em.persist(e)
        self.assertEqual(em._registry.values()[0].state, ADD)
        em.remove(e)
        self.assertEqual(em._registry.values()[0].state, REMOVE)

    def test_flush(self):
        em = self.em
        e = TestType({'foo': 'bar'})
        em.persist(e)
        e2 = TestType({'bar': 'baz'})
        em.persist(e2)
        em.flush()
        self.assertTrue('id' in e)
        self.assertTrue('id' in e2)
        self.assertTrue(all(map(lambda pe: pe.state == UPDATE, em._registry.values())))
        e2['bar'] = 'foo'
        self.assertTrue(all(map(lambda pe: pe.state == UPDATE, em._registry.values())))
        em.flush()

    def test_find(self):
        em = self.em
        em2 = self.em
        e = TestType({'foo': 'bar'})
        em.persist(e)
        em.flush()
        fe = em2.find(e['id'], TestType)
        self.assertDictEqual(e.to_representation(), fe.to_representation())
        self.assertRaises(EntityNotFound, em2.find, 'non-exists', TestType)

    def test_find_updated(self):
        em = self.em
        em2 = self.em
        e = TestType({'foo': 'bar'})
        em.persist(e)
        em.flush()
        e['bar'] = 'baz'
        em.flush()
        fe = em2.find(e['id'], TestType)
        self.assertDictEqual(e.to_representation(), fe.to_representation())

    def test_find_many(self):
        em = self.em
        em2 = self.em
        e = TestType({'foo': 'bar'})
        e2 = TestType({'bar': 'baz'})
        em.persist(e)
        em.persist(e2)
        em.flush()
        fe = em2.find_many([e['id'], e2['id']], TestType)
        self.assertDictEqual(e.to_representation(), fe[0].to_representation())
        self.assertDictEqual(e2.to_representation(), fe[1].to_representation())

    def test_query(self):
        em = self.em
        em2 = self.em
        e = TestType({'foo': 'value', 'bar': 'baz', 'baz': 'foo'})
        e2 = TestType({'foo': 'value', 'bar': 'baz', 'baz': 'foo'})
        e3 = TestType({'foo': 'value', 'bar': 'baz', 'baz': 'foo'})
        em.persist(e)
        em.persist(e2)
        em.persist(e3)
        em.flush()
        em.get_client().indices.refresh(index=self._index)
        fe, meta = em2.query({'query': {'term': {'foo': {'value': 'value'}}}}, TestType)
        self.assertEqual(len(fe), 3)

    def test_query_one(self):
        em = self.em
        em2 = self.em
        e = TestType({'foo': 'bar'})
        em.persist(e)
        em.flush()
        em.get_client().indices.refresh(index=self._index)
        fe = em2.query_one({'query': {'term': {'foo': {'value': 'bar'}}}}, TestType)
        self.assertEqual(fe['id'], e['id'])
        e2 = TestType({'foo': 'bar'})
        em.persist(e2)
        em.flush()
        em.get_client().indices.refresh(index=self._index)
        self.assertRaises(RepositoryError, em2.query_one, {'query': {'term': {'foo': {'value': 'bar'}}}}, TestType)

    def test_find_scope(self):
        em = self.em
        em2 = self.em
        e = TestType({'foo': 'bar', 'bar': 'baz'})
        em.persist(e)
        em.flush()
        fe = em2.find(e['id'], TestType, scope='all')
        self.assertDictEqual({'foo': 'bar', 'bar': 'baz'}, fe.to_representation())
        fe2 = em2.find(e['id'], TestType, scope='small')
        self.assertDictEqual({'foo': 'bar'}, fe2.to_representation())

    def test_find_many_scope(self):
        em = self.em
        em2 = self.em
        e = TestType({'foo': 'bar', 'bar': 'baz'})
        e2 = TestType({'foo': 'bar', 'bar': 'baz'})
        em.persist(e)
        em.persist(e2)
        em.flush()
        fe = em2.find_many([e['id'], e2['id']], TestType, scope='small')
        self.assertDictEqual({'foo': 'bar'}, fe[0].to_representation())
        self.assertDictEqual({'foo': 'bar'}, fe[1].to_representation())

    def test_query_scope(self):
        em = self.em
        em2 = self.em
        e = TestType({'foo': 'value', 'bar': 'baz', 'baz': 'foo'})
        e2 = TestType({'foo': 'value', 'bar': 'baz', 'baz': 'foo'})
        e3 = TestType({'foo': 'value', 'bar': 'baz', 'baz': 'foo'})
        em.persist(e)
        em.persist(e2)
        em.persist(e3)
        em.flush()
        em.get_client().indices.refresh(index=self._index)
        fe, meta = em2.query({'query': {'term': {'foo': {'value': 'value'}}}}, TestType, scope='small')
        self.assertDictEqual({'foo': 'value'}, fe[0].to_representation())
        self.assertDictEqual({'foo': 'value'}, fe[1].to_representation())
        self.assertDictEqual({'foo': 'value'}, fe[2].to_representation())

    def test_query_one_scope(self):
        em = self.em
        em2 = self.em
        e = TestType({'foo': 'value', 'bar': 'baz', 'baz': 'foo'})
        em.persist(e)
        em.flush()
        em.get_client().indices.refresh(index=self._index)
        fe = em2.query_one({'query': {'term': {'foo': {'value': 'bar'}}}}, TestType, scope='small')
        self.assertDictEqual({'foo': 'value'}, fe.to_representation())

    def test_timestamps(self):
        em = self.em
        e = TimestampsType({'foo': 'bar'})
        em.persist(e)
        em.flush()
        self.assertEqual(e['created_at'], e['updated_at'])
        e['baz'] = 'bar'
        em.flush()
        self.assertTrue(e['created_at'] < e['updated_at'])

    #  TODO: test get_repository