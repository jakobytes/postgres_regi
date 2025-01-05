from flask import request
from socket import gethostbyname, gethostname
import psycopg2
import time

import config

def create_logging_table(db):
    db.execute(
        'CREATE TABLE IF NOT EXISTS {}('
        '  log_id SERIAL PRIMARY KEY,'
        '  level VARCHAR(10) DEFAULT \'INFO\','
        '  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,'
        '  hostname VARCHAR(100) DEFAULT NULL,'
        '  msg VARCHAR(2000) DEFAULT NULL,'
        '  user_agent VARCHAR(1000) DEFAULT NULL,'
        '  crawler VARCHAR(50) DEFAULT NULL'
        ');'.format(config.LOGGING_TABLE_NAME))

def log(level, msg):
    if config.ENABLE_LOGGING_TO_DB:
        with psycopg2.connect(**config.POSTGRESQL_PARAMS) as db_con:
            with db_con.cursor() as db:
                db.execute('SELECT to_regclass(%s);', (config.LOGGING_TABLE_NAME,))
                if db.fetchone()[0] is None:
                    create_logging_table(db)
                if len(msg) > 2000:
                    msg = msg[:1997] + '...'
                db.execute('INSERT INTO {} (level, hostname, msg, user_agent) '
                           'VALUES (%s, %s, %s, %s)'\
                           .format(config.LOGGING_TABLE_NAME),
                           (level, gethostbyname(gethostname()), msg,
                            request.user_agent.string))
            db_con.commit()

def profile(fun):
    def exec_profiled_fun(*args, **kwargs):
        t1 = time.time()
        if config.BANNED_CRAWLERS and config.BANNED_CRAWLERS.search(request.user_agent.string):
            result = config.BANNED_CRAWLER_RESPONSE
        else:
            result = fun(*args, **kwargs)
        t2 = time.time()
        log('INFO', '{} {}.{} took {}s'.format(
                        '{}?{}'.format(request.path, request.query_string.decode()),
                        fun.__module__, fun.__name__, t2-t1))
        return result
    return exec_profiled_fun
