"""
Dead simple RDBMS handling library

Because I hate ORMs.

This library creates a simple manager object from a database connection.
The database connection must be configured to return a DictCursor when
requested for a cursor.

Manager API:
    - select
    - insert
    - update
    - delete
    - commit
    - rollback
    - connection
    - cursor

Example Usage:

    import MySQLdb
    import MySQLdb.cursors

    conn = MySQLdb.connect(host='localhost', user='root', db='lorem',
                           cursorclass=MySQLdb.cursors.DictCursor)

    # You can always pass a psycopg2 connection object as well, or any
    # connection object that implements the Python DB API 2.0 (PEP 0249)

    db = make_db_manager(conn)

    itemiter = db.select('products', 'id,name,description')
    item = itemiter.next()
    print item.name

    last_insert_id = db.insert('products', {
        'name': 'falan',
        'description': 'harika bir urun!!',
    })

    last_insert_id = db.insert('products', {
        'name': 'falan',
        'description': 'harika bir urun!!',
    }, commit=False)
    db.commit()

    affected_rowcount = db.update('products', {
        'name': 'lorem ipsum',
    },
    {
        'id =': 888,
    })

    affected_rowcount = db.delete('products', {
        'id =': 777,
    })
"""

from sys import stderr
from collections import namedtuple, OrderedDict
from functools import partial
from itertools import chain


def make(dbconn):
    """
    Factory method to create a database manager object that exposes the
    following API:

    - select
    - insert
    - update
    - delete
    - commit
    - rollback
    - connection
    - cursor

    :param dbconn:
        A connection object compatible with the Python DB 2.0 API.
        Note that it must be configured to return DictCursors instead of
        standard ones.
    """

    dbcursor = dbconn.cursor()

    dbmanagerdict = {
        'connection': dbconn,
        'cursor': dbcursor,
        'select': partial(query, 'select', dbcursor),
        'get': partial(query, 'get', dbcursor),
        'upsert': partial(query, 'upsert', dbcursor),
        'insert': partial(query, 'insert', dbcursor),
        'update': partial(query, 'update', dbcursor),
        'delete': partial(query, 'delete', dbcursor),
        'commit': lambda: dbconn.commit(),
        'rollback': lambda: dbconn.rollback(),
    }

    return namedtuple('dbmanager', dbmanagerdict.keys())(**dbmanagerdict)


def query(operation, dbcursor, *args, **kw):
    """
    :param operation: 'select' | 'upsert' | 'delete'
    :param dbcur: db cursor object
    :param debug: if True, dumps the query to stderr
    :param **params: params to be passed to the given operation
    """
    debug = False
    commit = True
    asdict = False

    if kw.has_key('debug'):
        debug = kw.pop('debug')

    if kw.has_key('commit'):
        commit = kw.pop('commit')

    if kw.has_key('asdict'):
        asdict = kw.pop('asdict')

    querytpl, params = get_query_builder(operation)(*args, **kw)

    if debug:
        stderr.write('%s\n%s' % (querytpl, params))
        return None

    dbcursor.execute(querytpl, params)

    if operation in ('insert', 'update', 'upsert', 'delete') and commit:
        dbcursor.connection.commit()

    if operation in ('select', 'get'):
        itemiter = iter(dbcursor)
        first_record = itemiter.next()

        if not first_record:
            return iter([])

        itemiter = chain([first_record], itemiter)

        if not asdict:
            Result = namedtuple('dbrecord', first_record.keys())
            itemiter = (Result(**item) for item in itemiter)

        if operation == 'get':
            return next(itemiter, None)
        else:
            return itemiter

    elif operation in ['update', 'upsert', 'delete']:
        return dbcursor.rowcount

    elif operation == 'insert':
        return dbcursor.lastrowid


def get_query_builder(operation):
    return {
        'select': build_select,
        'get': build_select,
        'upsert': build_upsert,
        'insert': build_upsert,
        'update': build_upsert,
        'delete': build_delete,
    }[operation]


def build_select(tablename, fieldlist, where=[], groups=[], having={},
                 order=[], limit=None):
    if isinstance(fieldlist, basestring):
        fieldlist = [f.strip() for f in fieldlist.split(',')]

    wheretpl, wherevalues = build_where_expr(where)
    havingtpl, havingvalues = build_where_expr(having, keyword='HAVING')

    querytpl = ' '.join([
        build_select_expr(tablename, fieldlist),
        wheretpl,
        build_group_expr(groups),
        havingtpl,
        build_order_expr(order),
        build_limit_expr(limit),
    ])

    return querytpl, wherevalues + havingvalues


def build_upsert(tablename, data, where=[], order=[], limit=None):
    upserttpl, upsertvalues = \
        build_update_expr(tablename, data) \
            if where else build_insert_expr(tablename, data)

    wheretpl, wherevalues = build_where_expr(where)

    querytpl = ' '.join([
        upserttpl,
        wheretpl,
        build_order_expr(order),
        build_limit_expr(limit),
    ])

    return querytpl, upsertvalues + wherevalues


def build_delete(tablename, where=[], order=[], limit=None):
    wheretpl, wherevalues = build_where_expr(where)

    querytpl = ' '.join([
        build_delete_expr(tablename),
        wheretpl,
        build_order_expr(order),
        build_limit_expr(limit),
    ])

    return querytpl, wherevalues


def build_select_expr(tablename, fieldlist):
    return 'SELECT %s FROM `%s`' % (', '.join(fieldlist),
                                    tablename)


def build_update_expr(tablename, data):
    data = OrderedDict(data)

    tpl = 'UPDATE `%s` SET %s' % (
        tablename,
        ', '.join('%s=%%s' % fieldname for fieldname in data.keys())
    )

    return tpl, data.values()


def build_insert_expr(tablename, data):
    if not isinstance(data, list):
        data = [data]

    data = [OrderedDict(item) for item in data]

    tpl = 'INSERT INTO `%s`(%s) VALUES %s' % (
        tablename,
        ', '.join(data[0].keys()),
        ', '.join(['(%s)' % item
                      for item in ([', '.join(['%s'] * len(data[0].keys()))] * len(data))]),
    )

    values = list(chain(*[item.values() for item in data]))

    return tpl, values


def build_delete_expr(tablename):
    return 'DELETE FROM `%s`' % tablename


def build_where_expr(conditions=[], keyword='WHERE'):
    if not conditions:
        return '', []

    if isinstance(conditions, (int, long)):
        conditions = [{'id =': conditions}]

    if isinstance(conditions, dict):
        conditions = [conditions]

    conditions = [OrderedDict(condition) for condition in conditions]

    tpl = ' OR '.join('(%s)' % ' AND '.join('%s %%s' % k for k in cond.keys())
                         for cond in conditions)

    values = list(chain(*(condition.values() for condition in conditions)))

    return '%s %s' % (keyword, tpl), values


def build_group_expr(groups=[]):
    if not groups:
        return ''

    return 'GROUP BY %s' % ', '.join(groups)


def build_order_expr(order=[]):
    if not order:
        return ''

    clauses = ['%s %s' % (fieldname.lstrip('-'), 'DESC' if fieldname[0] == '-'
                                                        else 'ASC')
                  for fieldname in order]

    return 'ORDER BY %s' % ', '.join(clauses)


def build_limit_expr(limit=None):
    if not limit:
        return ''

    return 'LIMIT %s' % limit
