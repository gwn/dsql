**Dead simple RDBMS handling library**

https://github.com/gwn/dsql

Dead simple SQL generation and result handling library. Designed to work with
Python DB API 2 compatible database connection objects.

Install with::

    pip install dsql

You can use `dsql` in two ways. First case is SQL statement generation::

    >>> from dsql import buildquery

    >>> buildquery('select',
                   'people',
                   ['name', 'surname'],
                   where=[{'age >': 30}],
                   orderby='-age',
                   dialect='postgresql')

    (
        'SELECT "name", "surname" FROM "people" WHERE "age" > %s ORDER BY "age" DESC',
        [30]
    )

Second use case is to create a manager object that, in addition to generating
your statements, automatically executes them and handles the results for you::

    >>> import psycopg2
    >>> from psycopg2.extras import DictCursor
    >>> from dsql import makemanager

    >>> conn = psycopg2.connect(database='foo', cursor_factory=DictCursor)

    >>> db = dsql.makemanager(conn, dialect='postgresql')

    >>> itemiter = db.select('products', where=[{'color =': 'red'}])

    >>> itemiter.next()
    {
        'id': 1,
        'title': 'Shirt',
        'color': 'red'
    }

    >>> db.insert('products', [{'title': 'Pants', 'color': 'green'},
    >>>                        {'title': 'Socks', 'color': 'yellow'}])
    [2, 3]

Note that *it is required* to configure the connection to return DictCursors
instead of standard cursors, as in the example above.

Check out the reference section below for the documentation of the whole API.

**Installation**::

    pip install dsql

**Reference**

Check out::

    # Query Builder

    query, params = dsql.buildquery(operation, tablename,
                                    <depends-on-the-operation>, ...
                                    dialect='standard')

    query, params = dsql.buildquery('select', tablename, fieldlist=[],
                                    where=[], groupby=[], having=[],
                                    orderby=[], limit=0, offset=0,
                                    dialect='standard')

    query, params = dsql.buildquery('insert', tablename, recordlist,
                                    dialect='standard')

    query, params = dsql.buildquery('update', tablename, updates, where=[],
                                    orderby=[], limit=0, offset=0,
                                    dialect='standard')

    query, params = dsql.buildquery('delete', tablename, where=[], orderby=[],
                                    dialect='standard')

    query, params = dsql.buildquery('raw', query, params)


    # Manager

    db = dsql.makemanager(dbapi2_compatible_connection_object,
                          dialect='standard')

    itemiter = db.select(tablename, fieldlist=[], where=[], groupby=[],
                         having=[], orderby=[], limit=1, offset=0, commit=True,
                         dry_run=False, response_handler=None)

    list_of_inserted_ids = db.insert(tablename, recordlist, commit=True,
                                     dry_run=False, response_handler=None)

    number_of_affected_rows = db.update(tablename, updates, where=[],
                                        orderby=[], limit=0, offset=0,
                                        commit=True, dry_run=False,
                                        response_handler=None)

    number_of_affected_rows = db.delete(tablename, where=[], orderby=[],
                                        commit=True, dry_run=False,
                                        response_handler=None)

    mixed = db.raw(query, params, commit=True, dry_run=False,
                   response_handler=None)
    # return value of this one depends on the type of query.

    related_connection_object = db.conn


Documentation of common parameters:

*fieldlist*

List of fields, such as `['name', 'age', 'occupation']`. Pass an empty list, or
skip altogether, to get all the fields.

*where*

List of condition groups.

Each condition group is a dict of predicate and value pairs, such as: `{'name
=': 'John', 'age >': 30}`. Each pair is combined with `AND`, so this example is
translated to the template `"name" = %s AND "age" >
%s` and values of `['John', 30]`.

Condition groups themselves are combined with `OR`, so the following `where`
expression::

  [{'name =': 'John', 'age >': 30}, {'occupation in': ['engineer', 'artist']}]

Translates to::

  WHERE ("name" = %s AND age > %s) OR (occupation IN (%s, %s))

with the values of: `['John', 30, 'engineer', 'artist']`

All standard comparison operators along with `LIKE`, `NOT LIKE`, `IN` and `NOT
IN` are supported.

If you need to construct more complicated filters, try raw queries.

*groupby*

List of group fields, such as `['age', 'occupation']`

*having*

Same as `where`.

*orderby*

List of fields to order by. Add the `-` prefix to field names for descending
order. Example: `['age', '-net_worth']`

*limit*

Limit as an integer, such as `50`.

*offset*

Offset as an integer, such as `200`.

*dialect*

`standard`, `postgresql` or `mysql`.

*commit*

Automatically commit the query. If you choose not to commit, you can always get
the connection object from the manager object (via `manager.conn`) and make the
commit yourself when the time is right.

*dry_run*

`True` or `False`. If `True`, does not execute the query, but dump it to the
standard error for inspecting.

*response_handler*

By default, the manager object handles the responses for you. It returns an
iterator of records for select calls, list of last inserted ids for insert
calls, and number of affected rows for others. In the cases you want to handle
the response yourself, you can pass your own `response_handler` whose arguments
will be the cursor object and the current dialect. Example::

    value_of_custom_handler = manager.select(tablename, limit=10,
                                             response_handler=custom_handler)


**Examples**

PosgreSQL with psycopg2::

    import psycopg2
    import psycopg2.extras
    import dsql

    conn = psycopg2.connect(host='localhost', user='root', database='lorem',
                            cursor_factory=psycopg2.extras.DictCursor)

    db = dsql.makemanager(conn, dialect='postgresql')

    itemiter = db.select('products', ['id', 'name', 'description'])
    item = itemiter.next()
    print item['name']

    ...

MySQL with MySQLdb::

    import MySQLdb
    import MySQLdb.cursors
    import dsql

    conn = MySQLdb.connect(host='localhost', user='root', db='lorem',
                           cursorclass=MySQLdb.cursors.DictCursor)

    db = dsql.makemanager(conn, dialect='mysql')

    itemiter = db.select('products',
                         ['id', 'name', 'description'],
                         where=[{'status =': 'in stock'}])
    item = itemiter.next()
    print item['name']

    last_insert_ids = db.insert('products',
                                [
                                    {
                                       'name': 'foo',
                                       'description': 'what a product!',
                                    }
                                ])

    last_insert_ids = db.insert('products',
                                [
                                   {
                                       'name': 'foo',
                                       'description': 'what a product!',
                                   }
                                ],
                                commit=False)
    db.conn.commit()

    affected_rowcount = db.update('products',
                                  {'name': 'lorem ipsum'},
                                  where=[{'id =': 888}])

    affected_rowcount = db.delete('products', where=[{'id =': 777}])


