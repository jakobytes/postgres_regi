import os
from operator import itemgetter
import pymysql
import warnings


MYSQL_PARAMS = {
    'host' : os.getenv('DB_HOST'),
    'port' : int(os.getenv('DB_PORT')),
    'user' : os.getenv('DB_USER'),
    'password' : os.getenv('DB_PASS'),
    'database' : os.getenv('DB_NAME')
}

VISUALIZATIONS_URL = os.getenv('VISUALIZATIONS_URL')

SEARCH_LIMIT = 1000
ENABLE_LOGGING_TO_DB = not not os.getenv('DB_LOGGING')
LOGGING_TABLE_NAME = 'runoregi_log'

# Info on whether the tables were found in the DB. Set on startup.
TABLES = {
  'collectors'    : False,
  'locations'     : False,
  'p_clust'       : False,
  'p_clust_freq'  : False,
  'p_col'         : False,
  'p_dupl'        : False,
  'p_loc'         : False,
  'p_sim'         : False,
  'p_year'        : False,
  'poem_theme'    : False,
  'poems'         : False,
  'raw_meta'      : False,
  'refs'          : False,
  'themes'        : False,
  'v_clust'       : False,
  'v_clust_freq'  : False,
  'v_clusterings' : False,
  'v_sim'         : False,
  'verse_poem'    : False,
  'verses'        : False,
  'verses_cl'     : False,
}

# Tables without which Runoregi can't work.
ESSENTIAL_TABLES = { 'poems', 'verse_poem', 'verses' }

# Error messages to print if a table is not found.
TABLE_ERRMSG = {
  'collectors'    : 'The collector information will not be available.',
  'locations'     : 'The place information will not be available.',
  'p_clust'       : 'The poem clustering will not be available.',
  'p_clust_freq'  : 'The poem clustering will not be available.',
  'p_col'         : 'The collector information will not be available.',
  'p_dupl'        : 'The poem duplicate information will not be available.',
  'p_loc'         : 'The place information will not be available.',
  'p_sim'         : 'The poem similarities will not be available.',
  'p_year'        : 'The year information will not be available.',
  'poem_theme'    : 'The type indices will not be available.',
  'poems'         : 'Terminating.',
  'raw_meta'      : 'The unstructured metadata will not be available.',
  'refs'          : 'The footnotes will not be available.',
  'themes'        : 'The type indices will not be available.',
  'v_clust'       : 'The verse clustering and passage search will not be available.',
  'v_clust_freq'  : 'The verse clustering and passage search will not be available.',
  'v_clusterings' : 'The verse clustering and passage search will not be available.',
  'v_sim'         : 'The neighboring clusters will not be available.',
  'verse_poem'    : 'Terminating.',
  'verses'        : 'Terminating.',
  'verses_cl'     : 'The side-by-side comparisons will not be available.',
}


def setup_tables():
    try:
        with pymysql.connect(**MYSQL_PARAMS) as db:
            db.execute('SHOW TABLES;')
            db_tables = set(map(itemgetter(0), db.fetchall()))
            for tbl in TABLES:
                TABLES[tbl] = tbl in db_tables
    except pymysql.err.OperationalError:
        raise RuntimeError('Cannot connect to the database. Terminating.')

    # print warnings or throw errors about non-existing tables
    for tbl in TABLES:
        if not TABLES[tbl]:
            if tbl in ESSENTIAL_TABLES:
                raise RuntimeError('Fatal: table `{}` table not found. {}'\
                                   .format(tbl, TABLE_ERRMSG[tbl]))
            else:
                warnings.warn('Table `{}` not found. {}'\
                              .format(tbl, TABLE_ERRMSG[tbl]))

def check_maintenance():
    with pymysql.connect(**MYSQL_PARAMS) as db:
        db.execute('SELECT SUM(ready != 1) FROM dbmeta;')
        result = db.fetchall()
        return result[0][0] > 0

