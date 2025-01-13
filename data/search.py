import re
import psycopg2
import config
from utils import render_type_links, render_xml


def extract_keywords(q):
    'Extract keywords from a search query.'
    keywords = [w.replace('*', '')  # Remove the wildcard for tsquery compatibility
        for w in re.findall(r'\w+\*?', q)  # Match words or wildcarded words
        if w.lower() not in ['and', 'or']]
    # Join keywords with '&' for AND search
    return ' & '.join(keywords)


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
    keywords = extract_keywords(q)
    db.execute(
        """
        SELECT nro, pos, type, text 
        FROM verses
        NATURAL JOIN verse_poem
        NATURAL JOIN poems
        WHERE to_tsvector('simple', text) @@ to_tsquery('simple', %s);
        """, (keywords,)
    )
    kwd = re.compile(keywords, flags=re.IGNORECASE)
    result = [(nro, pos, vtype, highlight(kwd, render_xml(text))) 
              for (nro, pos, vtype, text) in db.fetchall()]
    return result


def search_types(db, q):
    result = []
    if not config.TABLES.get('types'):
        return result

    keywords = extract_keywords(q)
    db.execute(
        """
        SELECT t4.name, t3.name, t2.name, t1.type_orig_id, t1.name, t1.description
        FROM types t1
        LEFT OUTER JOIN types t2 ON t1.par_id = t2.t_id
        LEFT OUTER JOIN types t3 ON t2.par_id = t3.t_id
        LEFT OUTER JOIN types t4 ON t3.par_id = t4.t_id
        WHERE to_tsvector('english', t1.name) @@ to_tsquery('english', %s)
        UNION
        SELECT t4.name, t3.name, t2.name, t1.type_orig_id, t1.name, t1.description
        FROM types t1
        LEFT OUTER JOIN types t2 ON t1.par_id = t2.t_id
        LEFT OUTER JOIN types t3 ON t2.par_id = t3.t_id
        LEFT OUTER JOIN types t4 ON t3.par_id = t4.t_id
        WHERE to_tsvector('english', t1.description) @@ to_tsquery('english', %s);
        """, (keywords, keywords)
    )

    result = [
        (
            r[3], 
            highlight(re.compile(keywords, flags=re.IGNORECASE), r[4]),
            highlight(re.compile(keywords, flags=re.IGNORECASE), render_type_links(render_xml(r[5]))),
            [r[i] for i in range(3) if r[i]]
        )
        for r in db.fetchall()
    ]

    return result


def search_meta(db, q):
    result = []
    if not config.TABLES.get('raw_meta'):
        return result
    keywords = extract_keywords(q)
    db.execute(
        """
        SELECT nro, field, value 
        FROM raw_meta
        NATURAL JOIN poems
        WHERE to_tsvector('english', value) @@ to_tsquery('english', %s);
        """, (keywords,)
    )
    result = [
        (nro, field, highlight(re.compile(keywords, flags=re.IGNORECASE), render_xml(value)))
        for nro, field, value in db.fetchall()
    ]

    return result


def search_smd(db, q):
    result = []

    keywords = extract_keywords(q)

    if config.TABLES['collectors'] and config.TABLES['p_col']:
        db.execute(
            """
            SELECT col_orig_id, name 
            FROM collectors
            WHERE to_tsvector('english', name) @@ to_tsquery('english', %s);
            """, (keywords,)
        )
        result.extend([('collector', col_id, highlight(re.compile(keywords, flags=re.IGNORECASE), name)) 
                       for col_id, name in db.fetchall()])
    
    if config.TABLES['places'] and config.TABLES['p_pl']:
        db.execute(
            """
            SELECT place_orig_id, name 
            FROM places
            WHERE to_tsvector('english', name) @@ to_tsquery('english', %s);
            """, (keywords,)
        )
        result.extend([('place', place_id, highlight(re.compile(keywords, flags=re.IGNORECASE), name)) 
                       for place_id, name in db.fetchall()])
    
    return result