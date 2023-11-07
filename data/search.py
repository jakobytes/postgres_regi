import re

import config
from utils import render_xml


def extract_keywords(q):
    'Extract keywords from a search query.'
    keywords = [w.replace('*', '\w*') \
        for w in re.findall('\w+\*?', q) \
        if w.lower() not in ['and', 'or', ]]
    return re.compile('|'.join(keywords), flags=re.IGNORECASE)


# FIXME this operates on strings, which are actually XML.
# If the search terms overlap with XML tags, this might lead to problems.
# To do it correctly, we would need to match the pattern only in
# the text within the XML.
def highlight(pattern, text):
    idx = 0
    result = []
    m = pattern.search(text, idx)
    while m is not None:
        result.append(text[idx:m.start()])
        result.append('<span class="selected">')
        result.append(text[m.start():m.end()])
        result.append('</span>')
        idx = m.end()
        m = pattern.search(text, idx)
    result.append(text[idx:])
    return ''.join(result)


def search_verses(db, q):
    result = []
    db.execute(\
        'SELECT nro, pos, text FROM verses'
        ' NATURAL JOIN verse_poem'
        ' NATURAL JOIN poems'
        ' WHERE MATCH(text) AGAINST(%s IN BOOLEAN MODE);', (q,))
    kwd = extract_keywords(q)
    result = [(nro, pos, highlight(kwd, render_xml(text))) \
              for (nro, pos, text) in db.fetchall()]
    return result


def search_types(db, q):
    result = []
    # ignore if the table is not available
    if not config.TABLES['types']:
        return result
    db.execute(\
      'SELECT t4.name, t3.name, t2.name, t1.type_orig_id, t1.name,'
      '       t1.description'
      ' FROM types t1'
      '  LEFT OUTER JOIN types t2 on t1.par_id = t2.t_id'
      '  LEFT OUTER JOIN types t3 on t2.par_id = t3.t_id'
      '  LEFT OUTER JOIN types t4 on t3.par_id = t4.t_id'
      '  WHERE MATCH(t1.name, t1.description) AGAINST(%s IN BOOLEAN MODE);', (q,))
    kwd = extract_keywords(q)
    result = [(r[3], highlight(kwd, r[4]), highlight(kwd, render_xml(r[5])),
               [r[i] for i in range(3) if r[i]]) \
              for r in db.fetchall()]
    return result


def search_meta(db, q):
    result = []
    # ignore if the table is not available
    if not config.TABLES['raw_meta']:
        return result
    db.execute(\
        'SELECT nro, field, value FROM raw_meta'
        ' NATURAL JOIN poems'
        ' WHERE MATCH(value) AGAINST(%s IN BOOLEAN MODE);', (q,))
    kwd = extract_keywords(q)
    result = [(nro, field, highlight(kwd, render_xml(value))) \
              for (nro, field, value) in db.fetchall()]
    return result

