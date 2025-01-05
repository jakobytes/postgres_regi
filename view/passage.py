from flask import render_template
import psycopg2
from urllib.parse import urlencode

import config
from data.logging import profile
from data.poems import Poems
from data.verses import get_clusterings, get_verses
from view.dendrogram import DEFAULTS as DENDROGRAM_DEFAULTS
from utils import link, print_type_list, render_csv

MAX_QUERY_LENGTH = None

DEFAULTS = {
  'nro': None,
  'start': 0,
  'end': 0,
  'clustering': 0,
  'context': 2,
  'dist': 2,
  'hitfact': 0.5,
  'format': 'html'
}

def generate_page_links(args, clusterings):
    global DEFAULTS

    def pagelink(**kwargs):
        return link('passage', dict(args, **kwargs), DEFAULTS)

    map_args = dict(DEFAULTS, **args)
    map_args['format'] = 'csv'
    del map_args['context']

    result = {
        'csv': pagelink(format='csv'),
        'tsv': pagelink(format='tsv'),
        'map_lnk' : config.VISUALIZATIONS_URL + '/?vis=map_passage&' \
                    + urlencode(map_args) \
                    if config.VISUALIZATIONS_URL else None,
        'dist': {}, 'hitfact': {}, 'context': {}, 'clustering': {}
    }
    for x in range(1, 7):
        result['dist'][x] = pagelink(dist=x)
    for x in range(1, 11, 2):
        result['context'][x] = pagelink(context=x)
    x = 0.1
    while x <= 1:
        xf = '{:.2}'.format(x)
        result['hitfact'][xf] = pagelink(hitfact=xf)
        x += 0.1
    for c in clusterings:
        result['clustering'][c[0]] = pagelink(clustering=c[0])
    return result

def filter_hits(verses, dist=2, min_hit_length=1):
    hits, cur_hit = [], []
    for v in verses:
        if not cur_hit:
            cur_hit = [v]
        else:
            if v.nro == cur_hit[-1].nro and v.pos-cur_hit[-1].pos <= dist:
                cur_hit.append(v)
            else:
                if len(cur_hit) >= min_hit_length:
                    hits.append(cur_hit)
                cur_hit = [v]
    if len(cur_hit) >= min_hit_length:
        hits.append(cur_hit)
    hits.sort(reverse=True, key=lambda h: (len(h), h[-1].pos-h[0].pos))
    return hits

@profile
def render(**args):
    if MAX_QUERY_LENGTH is not None and (args['end'] - args['start']) > MAX_QUERY_LENGTH:
        return '<b>Error:</b> passage length currently limited to {} verses!'\
               .format(MAX_QUERY_LENGTH)
    if args['end'] < args['start']:
        return '<b>Error:</b> passage end before the start!'
    with psycopg2.connect(**config.POSTGRESQL_PARAMS) as db_con:
        with db_con.cursor() as db:
            clusterings = get_clusterings(db)
            passage = get_verses(db, nro=args['nro'], start_pos=args['start'],
                                 end_pos=args['end'], clustering_id=args['clustering'])
            clust_ids = set(v.clust_id for v in passage)
            verses = get_verses(db, clust_id=tuple(clust_ids),
                                clustering_id=args['clustering'])
            verses.sort(key=lambda v: (v.nro, v.pos))
            min_hit_length = args['hitfact'] * (args['end']-args['start']+1)
            hits = filter_hits(verses, dist=args['dist'],
                               min_hit_length=min_hit_length)
            passages = [ 
                { 'verses':
                      get_verses(db, nro=h[0].nro,
                                 start_pos=h[0].pos-args['context'],
                                 end_pos=h[-1].pos+args['context'],
                                 clustering_id=args['clustering'])
                } for h in hits
            ]
            for pas in passages:
                pas['nro'] = pas['verses'][0].nro
                pas['matches'] = [v.pos for v in pas['verses'] if v.clust_id in clust_ids]
                pas['hl'] = (pas['verses'][0].nro == args['nro'] \
                             and pas['verses'][0].pos in \
                                 range(args['start']-args['context'], 
                                       args['end']+args['context']))
            poems = Poems(nros=[pas['verses'][0].nro for pas in passages])
            poems.get_structured_metadata(db)
            types = poems.get_types(db)
            types.get_names(db)

    if args['format'] in ('csv', 'tsv'):
        return render_csv([
            (pas['nro'], pas['verses'][0].pos,
             '\n'.join([v.text_norm for v in pas['verses']]),
             ';'.join(p.parish_id if p.parish_id is not None else p.county_id \
                      for p in poems[pas['nro']].smd.place_lst),
             poems[pas['nro']].smd.place,
             ';'.join(c.id for c in poems[pas['nro']].smd.collector_lst),
             poems[pas['nro']].smd.collector,
             ';'.join(poems[pas['nro']].type_ids),
             print_type_list(poems[pas['nro']], types)) \
            for pas in passages],
            header=('nro', 'pos', 'snippet', 'place_id', 'place',
                    'collector_id', 'collector', 'type_id', 'types'),
            delimiter='\t' if args['format'] == 'tsv' else ',')
    else:
        links = generate_page_links(args, clusterings)
        links['dendrogram'] = link('dendrogram',
            { 'source': 'nros', 'nro': ','.join(pas['nro'] for pas in passages) },
            DENDROGRAM_DEFAULTS)
        data = { 'passages': passages, 'poems': poems, 'types': types,
                 'clusterings': clusterings
        }
        return render_template('passage.html', args=args, data=data, links=links)
