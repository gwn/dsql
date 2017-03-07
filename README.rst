**Dead simple RDBMS handling library**

https://github.com/gwn/dsql

Dead simple wrapper for Python DB API 2 compatible database modules that
supports very basic SQL generation and result handling.

Because I hate ORMs.

This library creates a simple manager object from a database connection, that
exposes the following API:

- select
- insert
- update
- delete
- raw
- commit
- rollback
- connection
- cursor

**Philosophy**

Coming soon. It will be something like that interactions with relational
databases should be far more simpler that it currently is in the majority of
applications, and how I don't like ORMs and complex migration systems and stuff
like that.

**Installation**::

    pip install dsql

**Usage**

Any database connection object that implements the Python DB API 2.0 (PEP 0249)
is supported. Also, the database connection must be configured to return a
DictCursor when a cursor is requested.

Example Usage (mysql)::

    import MySQLdb
    import MySQLdb.cursors
    import dsql

    # Create your connection object with a DictCursor as the cursor class:
    conn = MySQLdb.connect(host='localhost', user='root', db='lorem',
                           cursorclass=MySQLdb.cursors.DictCursor)

    # Create the manager object with the dialect being `mysql`:
    db = dsql.make(conn, dialect='mysql')

    itemiter = db.select('products', 'id,name,description')
    item = itemiter.next()
    print item['name']

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

Example Usage (postgres)::

    import psycopg2
    import psycopg2.extras
    import dsql

    # Create your connection object with a DictCursor as the cursor factory:
    conn = psycopg2.connect(host='localhost', user='root', database='lorem',
                            cursor_factory=psycopg2.extras.DictCursor)

    # Create the manager object with the dialect being `posgresql`:
    db = dsql.make(conn, dialect='postgresql')

    itemiter = db.select('products', 'id,name,description')
    item = itemiter.next()
    print item['name']

    ...
