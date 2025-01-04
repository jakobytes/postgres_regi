import os
from operator import itemgetter
import psycopg2
import re
import warnings


POSTGRESQL_PARAMS = {
    'host': '86.50.252.165',
    'port': int('5432'),
    'user': 'csc',
    'password':'huhhoq-gecko0-qItwuq',
    'dbname': 'Elias'
}

VISUALIZATIONS_URL = os.getenv('VISUALIZATIONS_URL')

SEARCH_LIMIT = 1000
ENABLE_LOGGING_TO_DB = not not os.getenv('DB_LOGGING')
ENABLE_PROXY = not not os.getenv('PROXY')
LOGGING_TABLE_NAME = 'runoregi_log'

BANNED_CRAWLERS = re.compile('Bytespider')
BANNED_CRAWLER_RESPONSE = '''
This site is currently not serving requests from your crawler.
Please consult robots.txt and conform to it.
'''

TABLES = {
  'collectors': False,
  'p_clust': False,
  'p_clust_freq': False,
  'p_col': False,
  'p_dupl': False,
  'p_pl': False,
  'p_sim': False,
  'p_year': False,
  'places': False,
  'p_typ': False,
  'poems': False,
  'raw_meta': False,
  'refs': False,
  'runoregi_pages': False,
  'types': False,
  'v_clust': False,
  'v_clust_freq': False,
  'v_clusterings': False,
  'v_sim': False,
  'verse_poem': False,
  'verses': False,
  'verses_cl': False,
}

ESSENTIAL_TABLES = {'poems', 'verse_poem', 'verses'}

TABLE_ERRMSG = {
  'collectors': 'The collector information will not be available.',
  'p_clust': 'The poem clustering will not be available.',
  'p_clust_freq': 'The poem clustering will not be available.',
  'p_col': 'The collector information will not be available.',
  'p_dupl': 'The poem duplicate information will not be available.',
  'p_pl': 'The place information will not be available.',
  'p_sim': 'The poem similarities will not be available.',
  'p_year': 'The year information will not be available.',
  'places': 'The place information will not be available.',
  'p_typ': 'The type indices will not be available.',
  'poems': 'Terminating.',
  'raw_meta': 'The unstructured metadata will not be available.',
  'refs': 'The footnotes will not be available.',
  'runoregi_pages': 'Some Runoregi pages (especially the starting page)'
                     ' might be missing content.',
  'types': 'The type indices will not be available.',
  'v_clust': 'The verse clustering and passage search will not be available.',
  'v_clust_freq': 'The verse clustering and passage search will not be available.',
  'v_clusterings': 'The verse clustering and passage search will not be available.',
  'v_sim': 'The neighboring clusters will not be available.',
  'verse_poem': 'Terminating.',
  'verses': 'Terminating.',
  'verses_cl': 'The side-by-side comparisons will not be available.',
}


def setup_tables():
    try:
        with psycopg2.connect(**POSTGRESQL_PARAMS) as db_con:
            with db_con.cursor() as db:
                db.execute('SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != \'pg_catalog\' AND schemaname != \'information_schema\';')
                db_tables = set(row[0] for row in db.fetchall())
                for tbl in TABLES:
                    TABLES[tbl] = tbl in db_tables
    except psycopg2.OperationalError:
        raise RuntimeError('Cannot connect to the database. Terminating.')

    for tbl in TABLES:
        if not TABLES[tbl]:
            if tbl in ESSENTIAL_TABLES:
                raise RuntimeError('Fatal: table `{}` table not found. {}'.format(tbl, TABLE_ERRMSG[tbl]))
            else:
                warnings.warn('Table `{}` not found. {}'.format(tbl, TABLE_ERRMSG[tbl]))


def check_maintenance():
    with psycopg2.connect(**POSTGRESQL_PARAMS) as db_con:
        with db_con.cursor() as db:
            db.execute('SELECT SUM(ready != 1) FROM dbmeta;')
            result = db.fetchone()
            return result[0] > 0
