from collections import OrderedDict
from itertools import chain


def buildquery(operation, *args, **kw):
    """
    Return a query string and argument list pair such as:

        (
            'SELECT * FROM "sometable" WHERE "id" IN (?, ?)',
            (2, 3)
        )
    """

    builder = {
        'select': build_select_stmt,
        'insert': build_insert_stmt,
        'update': build_update_stmt,
        'delete': build_delete_stmt,
        'raw': build_raw_stmt,
    }[operation]

    return builder(*args, **kw)


def build_select_stmt(tablename, fieldlist=[], where=[], groupby=[], having=[],
                      orderby=[], limit=0, offset=0, dialect='standard'):
    """ """

    where_clause, where_params = build_where_clause(where, dialect=dialect)
    having_clause, having_params = build_having_clause(having, dialect=dialect)

    stmt = ' '.join([
        build_select_clause(fieldlist, dialect=dialect),
        build_from_clause(tablename, dialect=dialect),
        where_clause,
        build_groupby_clause(groupby, dialect=dialect),
        having_clause,
        build_orderby_clause(orderby, dialect=dialect),
        build_limit_clause(limit, offset, dialect=dialect),
    ])

    params = where_params + having_params

    return stmt, params


def build_insert_stmt(tablename, recordlist, dialect='standard'):
    """ """

    insert_clause, insert_params = build_insert_clause(tablename, recordlist,
                                                       dialect=dialect)

    if dialect == 'postgresql':
        insert_clause += ' returning id'

    return insert_clause, insert_params


def build_update_stmt(tablename, recordpatch, where=[], orderby=[], limit=0,
                      offset=0, dialect='standard'):
    """ """

    update_clause, update_params = build_update_clause(tablename, recordpatch,
                                                       dialect=dialect)
    where_clause, where_params = build_where_clause(where, dialect=dialect)

    stmt = ' '.join([
        update_clause,
        where_clause,
        build_orderby_clause(orderby, dialect=dialect),
        build_limit_clause(limit, offset, dialect=dialect),
    ])

    params = update_params + where_params

    return stmt, params


def build_delete_stmt(tablename, where=[], orderby=[], limit=0,
                      dialect='standard'):
    """ """

    where_clause, where_params = build_where_clause(where, dialect=dialect)

    stmt = ' '.join([
        'DELETE',
        build_from_clause(tablename, dialect=dialect),
        where_clause,
        build_orderby_clause(orderby, dialect=dialect),
        build_limit_clause(limit, dialect=dialect),
    ])

    params = where_params

    return stmt, params


def build_raw_stmt(stmt, params=[], dialect='standard'):
    return stmt, params


def build_select_clause(fieldlist=[], dialect='standard'):
    if fieldlist:
        fieldlist_str = ', '.join(quote_identifier(f, dialect=dialect)
                                      for f in fieldlist)
    else:
        fieldlist_str = '*'

    return 'SELECT %s' % fieldlist_str


def build_from_clause(tablename, dialect='standard'):
    return 'FROM %s' % quote_identifier(tablename, dialect=dialect)


def build_insert_clause(tablename, recordlist, dialect='standard'):
    recordlist = [OrderedDict(record) for record in recordlist]

    tpl = 'INSERT INTO %s(%s) VALUES %s' % (
              quote_identifier(tablename, dialect=dialect),
              ', '.join(quote_identifier(fieldname, dialect=dialect)
                           for fieldname in recordlist[0].keys()),
              ', '.join(['(%s)' % item
                            for item in ([', '.join(['%s'] * len(recordlist[0].keys()))] * len(recordlist))]),
          )

    params = list(chain(*[record.values() for record in recordlist]))

    return tpl, params


def build_update_clause(tablename, recordpatch, dialect='standard'):
    recordpatch = OrderedDict(recordpatch)

    tpl = 'UPDATE %s SET %s' % (
              quote_identifier(tablename, dialect=dialect),
              ', '.join('%s=%%s' % quote_identifier(fieldname, dialect=dialect)
                            for fieldname in recordpatch.keys())
          )

    return tpl, recordpatch.values()


def build_delete_clause(tablename, dialect='standard'):
    return 'DELETE FROM %s' % quote_identifier(tablename, dialect=dialect)


def build_where_clause(conditionlist=[], keyword='WHERE', dialect='standard'):
    """
    :param conditionlist: list

        List of conditions. Each condition is a dict of predicate-value pairs
        that will be combined with "AND". And the conditions themselves will be
        combined with "OR". Example:

            [
                {'name =': 'John', 'age >': 30},
                {'age <': 40}
            ]

        Which translates to:

            ("name" = 'John' AND "age" > 30) OR ("age" < 40)

    :param keyword: str
        `WHERE` | `HAVING`
    """

    if not conditionlist:
        return '', []

    conditionlist = [OrderedDict(condition) for condition in conditionlist]

    def build_condition_group(condition):
        return ' AND '.join(build_condition(predicate, value)
                                for predicate, value in condition.items())

    def build_condition(predicate, value):
        if ' ' not in predicate:
            raise Exception('The operator is missing in the predicate '
                            'expression!')

        fieldname, operator = predicate.split(' ', 1)

        if operator in ['in', 'not in'] and isinstance(value, list):
            placeholders = '(' + ','.join(['%s'] * len(value)) + ')'
        else:
            placeholders = '%s'

        return '%s %s %s' % (quote_identifier(fieldname, dialect=dialect),
                             validate_operator(operator),
                             placeholders)

    tpl = ' OR '.join(['(%s)' % build_condition_group(condition)
                         for condition in conditionlist])

    values = list(chain(*(flatten(condition.values(), depth=1)
                           for condition in conditionlist)))

    return '%s %s' % (keyword, tpl), values


def validate_operator(operator):
    supported_operators = (
        '=', '>', '<', '!=', '<=', '>=', 'in', 'not in', 'like', 'not like'
    )

    if operator not in supported_operators:
        raise ValueError('Non supported operator!')

    return operator


def build_having_clause(conditionlist=[], dialect='standard'):
    return build_where_clause(conditionlist, keyword='HAVING', dialect=dialect)


def build_groupby_clause(grouplist=[], dialect='standard'):
    if not grouplist:
        return ''

    return 'GROUP BY %s' % ', '.join(quote_identifier(g, dialect=dialect)
                                         for g in grouplist)


def build_orderby_clause(orderlist=[], dialect='standard'):
    if not orderlist:
        return ''

    subclauses = ['%s %s' % (quote_identifier(fieldname.lstrip('-'),
                                              dialect=dialect),
                             'DESC' if fieldname[0] == '-' else 'ASC')
                     for fieldname in order]

    return 'ORDER BY %s' % ', '.join(subclauses)


def build_limit_clause(limit=0, offset=0, dialect='standard'):
    if not limit:
        return ''

    if dialect == 'postgresql':
        clausetpl = 'OFFSET %s LIMIT %s'
    else:
        clausetpl = 'LIMIT %s, %s'

    return clausetpl % (offset, limit)


def quote_identifier(identifier, dialect='standard'):
    templates = {'standard': '"%s"',
                 'postgresql': '"%s"',
                 'mysql': '`%s`'}

    return templates[dialect] % identifier


def flatten(lst, depth=0, level=0):
    """ Utility to flatten lists with the option to constrain by depth """

    if depth and depth == level:
        return lst

    flatlist = []

    for item in lst:
        if isinstance(item, list):
            flatlist.extend(flatten(item, depth, level + 1))
        else:
            flatlist.append(item)

    return flatlist
