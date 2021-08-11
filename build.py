import sys
from inspect import signature

from unscne import ner, load_meta
from unscne.graph import connect_graph
from unscne.ner import link_dbpedia_with_wikidata, annotate_dbpedia_spotlight_to_sentences, annotate_speech_country2
from unscne.util import log, LogLevel

graph = connect_graph()
function_map = {
    "make": [graph.create_indices_and_constraints],
    "metadata": load_meta.load_metadata_into_graph,
    "meta": load_meta.load_metadata_into_graph,
    "sentences": load_meta.load_sentences_into_graph,
    "sent": load_meta.load_sentences_into_graph,
    "link_meta": load_meta.link_meta_to_speeches,
    "link_text": load_meta.link_paragraph_and_sentence_to_speakers,
    "president": load_meta.add_president_label,
    "country": annotate_speech_country2,
    "annotate_dbpedia": annotate_dbpedia_spotlight_to_sentences,
    "link_dbpedia": link_dbpedia_with_wikidata,
    "next_speech": load_meta.create_next_speech_relation,
    "next_sentence": load_meta.create_next_sentence_relation,
    "next_paragraph": load_meta.create_next_paragraph_relation,
    "next": [load_meta.create_next_paragraph_relation, load_meta.create_next_sentence_relation,
             load_meta.create_next_speech_relation],
    "class": [ner.get_classes_for_wd],
    "speech_to_nodes": load_meta.add_speech_meta_to_nodes,
    "agenda": load_meta.unify_agenda_relation
}


def call_with_adjusted_args(func):
    log(f"Calling {func.__name__}")
    if len(signature(func).parameters) == 0:
        func()
    else:
        func(graph)


def execute(command: str):
    func = function_map[command]
    if isinstance(func, list):
        for f in func:
            call_with_adjusted_args(f)
    else:
        call_with_adjusted_args(func)


unknown = set()
if len(sys.argv) > 1:
    for arg in sys.argv[1:]:
        if arg in function_map.keys():
            execute(arg)
        else:
            unknown.add(arg)
    if len(unknown) >= 1:
        log(
            f"Available commands: {', '.join(function_map.keys())}\n{len(unknown)} unknown command(s): {', '.join(unknown)}",
            LogLevel.WARNING)

else:
    # python build.py make meta sent link_meta next speech_to_nodes link_text president country orga annotate_dbpedia link_dbpedia documents
    graph.create_indices_and_constraints()
    load_meta.load_metadata_into_graph(graph)

    load_meta.load_sentences_into_graph(graph)

    load_meta.link_meta_to_speeches(graph)
    load_meta.create_next_speech_relation(graph)
    load_meta.create_next_sentence_relation(graph)
    load_meta.create_next_paragraph_relation(graph)

    load_meta.add_speech_meta_to_nodes(graph)
    load_meta.link_paragraph_and_sentence_to_speakers(graph)
    load_meta.add_president_label(graph)
    annotate_speech_country2(graph)
    # run one
    annotate_dbpedia_spotlight_to_sentences(graph)

    link_dbpedia_with_wikidata(graph)
