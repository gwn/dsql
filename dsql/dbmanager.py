from sys import stderr
from collections import namedtuple, OrderedDict
from functools import partial
from itertools import chain


def make(dbconn, dialect='standard'):
    """
    Factory method to create a database manager object that exposes the
    following API:

    - select
    - get
    - insert
    - update
    - delete
    - raw
    - commit
    - rollback
    - connection
    - cursor

    :param dbconn:
        A connection object compatible with the Python DB 2.0 API.
        Note that it must be configured to return DictCursors instead of
        standard ones.

    :param dialect:
        "standard" | "mysql" | "mssql" | "postgresql"
    """

    def make():
        dbmanagerdict = {
            'connection': dbconn,
            'select': partial(query, 'select'),
            'get': partial(query, 'get'),
            'upsert': partial(query, 'upsert'),
            'insert': partial(query, 'insert'),
            'update': partial(query, 'update'),
            'delete': partial(query, 'delete'),
            'raw': partial(query, 'raw'),
            'commit': dbconn.commit,
            'rollback': dbconn.rollback,
        }

        return namedtuple('dbmanager', dbmanagerdict.keys())(**dbmanagerdict)


    def query(operation, *args, **kw):
        """
        :param operation: 'select' | 'upsert' | 'delete'
        :param dbcur: db cursor object
        :param debug: if True, dumps the query to stderr
        :param **params: params to be passed to the given operation
        """
        debug = False
        commit = True

        if kw.has_key('debug'):
            debug = kw.pop('debug')

        if kw.has_key('commit'):
            commit = kw.pop('commit')

        dbcursor = dbconn.cursor()

        querytpl, params = get_query_builder(operation)(*args, **kw)

        if debug:
            stderr.write('%s\n%s' % (querytpl, params))
            return None

        dbcursor.execute(querytpl, params)

        if commit:
            dbcursor.connection.commit()

        if operation == 'insert' and dialect == 'postgresql':
            return [rec[0] for rec in dbcursor]

        if dbcursor.description:
            itemiter = iter(dbcursor)
            first_record = next(itemiter, None)

            if not first_record:
                return None if operation == 'get' else iter([])

            itemiter = chain([first_record], itemiter)

            # Fix for psycopg2.extras.DictCursor, which does not
            # return a dict, but a dict-like object
            if not isinstance(first_record, dict):
                itemiter = (dict(item.items()) for item in itemiter)

            if operation == 'get':
                return next(itemiter, None)
            else:
                return itemiter

        elif dbcursor.lastrowid:
            total_items = 1 if isinstance(args[1], dict) else len(args[1])

            if dialect == 'mysql':
                return range(dbcursor.lastrowid,
                             dbcursor.lastrowid + total_items)
            else:
                return range(dbcursor.lastrowid - total_items,
                             dbcursor.lastrowid)

        else:
            return dbcursor.rowcount


    def get_query_builder(operation):
        return {
            'raw': build_raw,
            'select': build_select,
            'get': build_select,
            'insert': build_insert,
            'update': build_update,
            'delete': build_delete,
        }[operation]


    def build_raw(querytpl, params=[]):
        return querytpl, params


    def build_select(tablename, fieldlist=None, where=[], groups=[], having={},
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


    def build_insert(tablename, data):
        inserttpl, insertvalues = build_insert_expr(tablename, data)

        if dialect == 'postgresql':
            inserttpl += ' returning id'

        return inserttpl, insertvalues


    def build_update(tablename, data, where=[], order=[], limit=None):
        updatetpl, updatevalues = build_update_expr(tablename, data)
        wheretpl, wherevalues = build_where_expr(where)

        querytpl = ' '.join([
            updatetpl,
            wheretpl,
            build_order_expr(order),
            build_limit_expr(limit),
        ])

        return querytpl, updatevalues + wherevalues


    def build_delete(tablename, where=[], order=[], limit=None):
        wheretpl, wherevalues = build_where_expr(where)

        querytpl = ' '.join([
            build_delete_expr(tablename),
            wheretpl,
            build_order_expr(order),
            build_limit_expr(limit),
        ])

        return querytpl, wherevalues


    def quote_identifier(identifier):
        templates = {'standard': '"%s"',
                     'postgresql': '"%s"',
                     'mysql': '`%s`',
                     'mssql': '[%s]'}

        return templates[dialect] % identifier


    def build_select_expr(tablename, fieldlist):
        if fieldlist:
            fieldlist_str = ', '.join(quote_identifier(f) for f in fieldlist)
        else:
            fieldlist_str = '*'

        return 'SELECT %s FROM %s' % (fieldlist_str, quote_identifier(tablename))


    def build_update_expr(tablename, data):
        data = OrderedDict(data)

        tpl = 'UPDATE %s SET %s' % (
            quote_identifier(tablename),
            ', '.join('%s=%%s' % quote_identifier(fieldname)
                          for fieldname in data.keys())
        )

        return tpl, data.values()


    def build_insert_expr(tablename, data):
        if not isinstance(data, list):
            data = [data]

        data = [OrderedDict(item) for item in data]

        tpl = 'INSERT INTO %s(%s) VALUES %s' % (
            quote_identifier(tablename),
            ', '.join(quote_identifier(fieldname)
                         for fieldname in data[0].keys()),
            ', '.join(['(%s)' % item
                          for item in ([', '.join(['%s'] * len(data[0].keys()))] * len(data))]),
        )

        values = list(chain(*[item.values() for item in data]))

        return tpl, values


    def build_delete_expr(tablename):
        return 'DELETE FROM %s' % quote_identifier(tablename)


    def build_where_expr(conditions=[], keyword='WHERE'):
        if not conditions:
            return '', []

        if isinstance(conditions, (int, long)):
            conditions = [{'id =': conditions}]

        if isinstance(conditions, dict):
            conditions = [conditions]

        conditions = [OrderedDict(condition) for condition in conditions]

        def build_single_comparation(predicate, value):
            if ' ' in predicate:
                field, op = predicate.split(' ', 1)
            else:
                field, op = predicate, '='

            if op in ['in', 'not in'] and isinstance(value, list):
                placeholders = '(' + ','.join(['%s'] * len(value)) + ')'
            else:
                placeholders = '%s'

            return '%s %s %s' % (quote_identifier(field),
                                 validate_operator(op),
                                 placeholders)

        def build_comparation_group(cond):
            return ' AND '.join(build_single_comparation(predicate, value)
                                   for predicate, value in cond.items())

        def flatten(inlist):
            outlist = []

            for item in inlist:
                if isinstance(item, list):
                    outlist.extend(item)
                else:
                    outlist.append(item)

            return outlist

        tpl = ' OR '.join(['(%s)' % build_comparation_group(cond) for cond in conditions])

        values = list(chain(*(flatten(cond.values()) for cond in conditions)))

        return '%s %s' % (keyword, tpl), values


    def validate_operator(op):
        supported_operators = (
            '=', '>', '<', '!=', '<=', '>=', 'in', 'not in', 'like', 'not like'
        )

        if op not in supported_operators:
            raise ValueError('Non supported operator!')

        return op


    def build_group_expr(groups=[]):
        if not groups:
            return ''

        return 'GROUP BY %s' % ', '.join(quote_identifier(g) for g in groups)


    def build_order_expr(order=[]):
        if not order:
            return ''

        clauses = ['%s %s' % (quote_identifier(fieldname.lstrip('-')),
                              'DESC' if fieldname[0] == '-' else 'ASC')
                      for fieldname in order]

        return 'ORDER BY %s' % ', '.join(clauses)


    def build_limit_expr(limit=None):
        if not limit:
            return ''

        return 'LIMIT %s' % int(limit)


    return make()
