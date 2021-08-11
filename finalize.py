import sys

from unscne.graph import connect_graph
from unscne.ner import link_dbpedia_with_wikidata, get_classes_for_wd, get_class_hierarchy_for_wd, get_label_for_wd

graph = connect_graph()
force = False
if "-force" in sys.argv:
    force = True

link_dbpedia_with_wikidata(graph, force)
get_classes_for_wd(graph)

get_class_hierarchy_for_wd(graph)
get_label_for_wd(graph)
