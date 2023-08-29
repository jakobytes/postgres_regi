from collections import defaultdict
from flask import render_template
from operator import itemgetter
import pymysql
from urllib.parse import urlencode

import config
from data.logging import profile
from data.poems import Poems
from data.types import Types, render_type_tree


DEFAULTS = { 'id': None }


@profile
def render(**args):
    upper = Types(ids=[args['id']])
    poems = None
    with pymysql.connect(**config.MYSQL_PARAMS).cursor() as db:
        nros, minor_nros = upper.get_poem_ids(db, minor=True)
        if nros or minor_nros:
            poems = Poems(nros=nros+minor_nros)
            poems.get_structured_metadata(db)
        upper.get_descriptions(db)
        upper.get_ancestors(db, add=True)
        upper.get_names(db)
        subcat = Types(ids=[args['id']])
        subcat.get_descriptions(db)
        subcat.get_descendents(db, add=True)
        subcat.get_names(db)

    tree = render_type_tree(subcat)
    # remove the top hierarchy level from the tree
    tree.pop(0)
    for line in tree:
        line.prefix.pop(0)
    data = {
        'types': Types(types=[upper[t] for t in upper] + \
                             [subcat[t] for t in subcat if t != args['id']]),
        'tree': tree,
        'poems': poems,
        'minor': set(minor_nros),
        'maintenance': config.check_maintenance()
    }
    links = {
      'map': config.VISUALIZATIONS_URL + '/?vis=map_type&' \
             + urlencode({'type_orig_id': args['id']}) \
             if config.VISUALIZATIONS_URL else None,
      'cooc-types': config.VISUALIZATIONS_URL + '?vis=tree_types_cooc&' \
             + urlencode({'type_orig_id': args['id'], 'include_erab_orig': False}) \
             if config.VISUALIZATIONS_URL else None
    }
    return render_template('type.html', args=args, data=data, links=links)

