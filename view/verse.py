from collections import defaultdict, OrderedDict
from flask import render_template
import psycopg2
from urllib.parse import urlencode

import config
from data.logging import profile
from data.poems import Poems
from data.verses import \
    get_clusterings, get_verses, get_verse_cluster_neighbors
from utils import print_type_list, render_csv


DEFAULTS = {
  'format': 'html',
  'nro': None,
  'pos': 0,
  'v_id': 0,
  'clustering': 0
}


@profile
def render(**args):

    def _group_by_source(verses):
        results = OrderedDict()
        for v in verses:
            if v.nro not in results:
                results[v.nro] = []
            results[v.nro].append(v)
        return results

    def _group_nbclust(verses_nbclust, verses):
        nbclust, seen_clust = defaultdict(list), set()
        v_id_to_nro = { v.v_id: v.nro for v in verses }
        for v1_id, v2, sim in verses_nbclust:
            if v2.clust_id not in seen_clust:
                nbclust[v_id_to_nro[v1_id]].append(v2)
                seen_clust.add(v2.clust_id)
        return nbclust

    with psycopg2.connect(**config.POSTGRESQL_PARAMS) as db_con:
        with db_con.cursor() as db:
            verse = get_verses(
                db, nro=args['nro'], start_pos=args['pos'],
                end_pos=args['pos'], clustering_id=args['clustering'])[0]
            verses = get_verses(db, clust_id=(verse.clust_id,),
                                clustering_id=args['clustering'])
            verses_by_src = _group_by_source(verses)
            poems = Poems(nros=list(verses_by_src.keys()))
            poems.get_structured_metadata(db)
            types = poems.get_types(db)
            types.get_names(db)
            verses_nbclust = get_verse_cluster_neighbors(
                db, (verse.clust_id,), clustering_id=args['clustering'])
            clusterings = get_clusterings(db)

    nbclust = _group_nbclust(verses_nbclust, verses)

    if args['format'] in ('csv', 'tsv'):
        return render_csv([
            (v.nro, v.pos, v.text_norm,
             ';'.join(p.parish_id if p.parish_id is not None else p.county_id \
                      for p in poems[v.nro].smd.place_lst),
             poems[v.nro].smd.place,
             ';'.join(c.id for c in poems[v.nro].smd.collector_lst),
             poems[v.nro].smd.collector,
             ';'.join(poems[v.nro].type_ids),
             print_type_list(poems[v.nro], types))
            for v in verses],
            header=('nro', 'pos', 'text', 'place_id', 'place',
                    'collector_id', 'collector', 'type_id', 'types'),
            delimiter='\t' if args['format'] == 'tsv' else ',')
    else:
        data = {
            'verse': verse,
            'verses': verses_by_src,
            'poems': poems,
            'types': types,
            'nbclust': nbclust,
            'clusterings': clusterings
        }
        map_args = { 'nro': args['nro'], 'pos': args['pos'],
                     'clustering': args['clustering'] }
        links = {
            'map': config.VISUALIZATIONS_URL + '/?vis=map_cluster&' \
                   + urlencode(map_args) \
                   if config.VISUALIZATIONS_URL else None,
            'types': config.VISUALIZATIONS_URL + '/?vis=tree_types_cluster&' \
                     + urlencode(dict(map_args, incl_erab_orig=False)) \
                     if config.VISUALIZATIONS_URL else None
        }
        return render_template('verse.html', args=args, data=data, links=links)
