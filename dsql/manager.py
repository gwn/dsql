from sys import stderr
from collections import namedtuple
from functools import partial
from itertools import chain
from querybuilder import buildquery


def makemanager(conn, dialect='standard'):
    """
    Create a database manager object with the following API:

    `select`, `insert`, `update`, `delete`, `raw`, `conn`

    :param conn: object
        A connection object compatible with the Python DB 2.0 API.
        Note that it must be configured to return DictCursors instead of
        standard ones.

    :param dialect: str
        "standard" | "mysql" | "mssql" | "postgresql"

    :returns: object
        A object exposing the API mentioned above.
    """

    managerdict = {
        'select': partial(query, 'select', conn=conn, dialect=dialect),
        'insert': partial(query, 'insert', conn=conn, dialect=dialect),
        'update': partial(query, 'update', conn=conn, dialect=dialect),
        'delete': partial(query, 'delete', conn=conn, dialect=dialect),
        'raw': partial(query, 'raw', conn=conn, dialect=dialect),
        'conn': conn,
    }

    return namedtuple('dbmanager', managerdict.keys())(**managerdict)


def query(operation, *args, **kw):
    """
    :param operation: str
        "select" | "insert" | "update" | "delete" | "raw"

    :param *args:
        Additional positional arguments to be relayed to the `querybuilder`
        function. Check out its documentation for more info.

    :param **kw:
        Additional keyword arguments to be relayed to the `querybuilder`
        function. Check out its documentation for more info.

        Except that, the following keys (if exist) will be extracted for use of
        this function, and will not be relayed:

        connection: connection object

        commit: bool
            Whether to commit the changes or not.

        dry_run: bool
            If True, dump the resulting query to stderr without executing.

        response_handler: callable
            An optional callable that is used to extract the response from the
            cursor after the execution is completed. It is passed the database
            cursor object and the dialect. Its return value will be used as the
            return value of the this `query` function. Here is the signature:

            response_handler(cursor, dialect)

            Optional. A default handler is provided.
    """

    dialect = kw.get('dialect', 'standard')

    conn = kw.pop('conn')
    commit = kw.pop('commit', True)
    dry_run = kw.pop('dry_run', False)
    response_handler = kw.pop('response_handler', handle_response)

    cursor = conn.cursor()

    querytpl, queryparams = buildquery(operation, *args, **kw)

    if dry_run:
        stderr.write('%s\n%s' % (querytpl, queryparams))
        return None

    cursor.execute(querytpl, queryparams)

    if commit:
        conn.commit()

    return response_handler(cursor, dialect)


def handle_response(cursor, dialect):
    if cursor.lastrowid:
        handler = handle_insert_response

    elif cursor.description:
        handler = handle_select_response

    else:
        handler = handle_other_response

    return handler(cursor, dialect)


def handle_select_response(cursor, dialect):
    """
    :param cursor: cursor object
    :returns: iterator
    """

    itemiter = iter(cursor)
    first_record = next(itemiter, None)

    if not first_record:
        return itemiter

    itemiter = chain([first_record], itemiter)

    # Fix for psycopg2.extras.DictCursor, and the like, which does not
    # return a dict, but a dict-like object
    if not isinstance(first_record, dict):
        itemiter = (dict(item.items()) for item in itemiter)

    return itemiter


def handle_insert_response(cursor, dialect):
    """
    :param cursor: cursor object
    :returns: list
    """

    if dialect == 'postgresql':
        # PostgreSQL will return the inserted ids as the result of the
        # `returning` expression.
        return [record[0] for record in cursor]

    if dialect == 'mysql':
        # MySQL returns the first inserted row id when multiple rows are
        # inserted
        return range(cursor.lastrowid, cursor.lastrowid + cursor.rowcount)

    return range(cursor.lastrowid - cursor.rowcount, cursor.lastrowid)


def handle_other_response(cursor, dialect):
    return cursor.rowcount
