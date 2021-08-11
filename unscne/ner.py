import csv
import sys
from pathlib import Path
from typing import List, Union, Dict, Any

import requests
from tqdm import tqdm

from config import DBPEDIA_TO_WIKIDATA, WD_CLASSES, URL_TO_DBPEDIA_SERVICE, WD_GFS_ENDPOINT, \
    URL_TO_DBPEDIA_ENDPOINT, DBPEDIA_NERS, PARAGRAPH_META, WD_LABELS, WD_HIERARCHY, DBPEDIA_TO_WIKIDATA_INTERNAL, \
    DBPEDIA_TO_WIKIDATA_AMBIGUOUS, WD_SPARQL_ENDPOINT, COUNTRY_MAPPING
from unscne.load_meta import inject_sids_from_pids
from unscne.graph import HelloWorldExample
from tqdm.auto import tqdm

from unscne.util import timer, create_retrying_session, dump_tsv, load_tsv, load_file, log, LogLevel


def request_dbpedia_ners_from_text(text: str, key_mapping: Dict[str, str]) -> List[Dict[str, Union[str, None]]]:
    response = requests.get(URL_TO_DBPEDIA_SERVICE, params={"text": text}, headers={"accept": "application/json"})
    if response:
        json = response.json()
        if "Resources" in json.keys():
            for result in json["Resources"]:
                entry = {}
                for dbpedia_key, neo4j_key in key_mapping.items():
                    entry[neo4j_key] = result.get(dbpedia_key, None)
                yield entry


def extract_dbpedia_ners_from_text(text: str, key_mapping: Dict[str, str]) -> List[Dict[str, Union[str, None]]]:
    for entry in request_dbpedia_ners_from_text(text, key_mapping):
        del entry["types"]
        yield entry


def _make_load_dbpedia_dump_statement(appendix, filename):
    load_from_csv = """
    USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///%s' AS row
        FIELDTERMINATOR "\t"
        MATCH (p:Paragraph) WHERE p.id = row.p_id
        MERGE (d:DBConcept {uri : row.uri})

            CREATE (p)-[
        :MENTIONS {surfaceForm: row.surfaceForm, support : row.support, offset : row.offset, similarityScore : row.similarityScore, percentageOfSecondRank : row.percentageOfSecondRank}
        ]->(d)
        """
    return (load_from_csv + appendix) % filename


def make_load_dbpedia_dump_statement_with_types(filename):
    appendix = """
        MERGE (w:WDConcept {uri : row.type})
        MERGE (d)-[:rdf_type]->(w)
        
    """
    return _make_load_dbpedia_dump_statement(appendix, filename)


def make_load_dbpedia_dump_statement_without_types(filename):
    return _make_load_dbpedia_dump_statement("", filename)


def check_if_sids_in_ners_inject_if_not():
    tmp = load_tsv(DBPEDIA_NERS)
    if len(tmp) == 0:
        log(f"{DBPEDIA_NERS} is empty, cannot write any NEs", LogLevel.WARNING)
    elif "s_id" in tmp[0].keys():
        return
    else:
        log("No sentences ids present, injecting..")
        inject_sids_from_pids(DBPEDIA_NERS)
        log("Done.")


@timer
def write_dbpedia_annotations_to_graph(graph: HelloWorldExample):
    log("Annotating sentences with dbpedia..")
    check_if_sids_in_ners_inject_if_not()
    statement = f"""
    USING PERIODIC COMMIT 5000
    LOAD CSV WITH HEADERS FROM 'file:///{DBPEDIA_NERS}' AS row
    FIELDTERMINATOR "\t"
    MATCH (s:Sentence)
    WHERE s.id = row.s_id
    MERGE (d:DBConcept {{uri : row.uri}})
    CREATE (s)-[
        :MENTIONS {{surfaceForm: row.surfaceForm, support : row.support, offset : row.offset, similarityScore : row.similarityScore, percentageOfSecondRank : row.percentageOfSecondRank}}
    ]->(d)
    """
    graph.execute_query_without_transaction(statement)
    log("Done.")


def annotate_dbpedia_spotlight_to_sentences(graph: HelloWorldExample):
    if not Path(DBPEDIA_NERS).exists():
        log(f"File with DBpedia annotations does not exist ({DBPEDIA_NERS}).", LogLevel.WARNING)
    else:
        write_dbpedia_annotations_to_graph(graph)


def collect_pid_from_file(loaded, pid_key="p_id"):
    return set(e[pid_key] for e in loaded)


def load_paragraph_meta_without_double_p_ids(sth, pid_key="p_id"):
    doggu = set()
    hottu = list()
    for e in sth:
        if e[pid_key] in doggu:
            continue
        else:
            hottu.append(e)
            doggu.update(e[pid_key])
    return hottu


def make_dbpedia_dump():
    DBPEDIA_KEY_MAPPING = {"@URI": "uri",
                           "@support": "support",
                           "@types": "types",
                           "@surfaceForm": "surfaceForm",
                           "@offset": "offset",
                           "@similarityScore": "similarityScore",
                           "@percentageOfSecondRank": "percentageOfSecondRank"
                           }
    paragraph_paths = load_tsv(PARAGRAPH_META)
    already_parsed = set()
    log("Making dbpedia dump..")
    prev_run = load_tsv(DBPEDIA_NERS) if Path(DBPEDIA_NERS).exists() else []
    already_parsed = collect_pid_from_file(prev_run)
    sth = load_paragraph_meta_without_double_p_ids(paragraph_paths)
    todo = list(filter(lambda elem: not elem["p_id"] in already_parsed, sth))
    if len(already_parsed):
        log(f"Found {len(already_parsed)} already annotated sentences, {len(todo)} left to do..")

    with open(DBPEDIA_NERS, "w", encoding="utf-8") as outf:
        header = ["p_id", "uri", "paragraph_path", "support", "surfaceForm", "offset", "similarityScore",
                  "percentageOfSecondRank"]
        without_writer = csv.DictWriter(outf, header, delimiter="\t")
        without_writer.writeheader()
        for e in prev_run:
            without_writer.writerow(e)
        for paragraph_meta in tqdm(todo):
            p_id = paragraph_meta["p_id"]
            path = paragraph_meta["paragraph_path"]
            paragraph = load_file(path)
            for ner in extract_dbpedia_ners_from_text(paragraph, DBPEDIA_KEY_MAPPING):
                ner["p_id"] = p_id
                ner["paragraph_path"] = path
                without_writer.writerow(ner)


def filter_for_wikidata_concepts(candidates: List[str]):
    return [uri for uri in candidates if uri.startswith("http://www.wikidata.org/entity/Q")]


def get_wikidata_entry_from_global_thingy(uri, http):
    response = http.get(WD_GFS_ENDPOINT, params={"s": uri},
                        headers={"accept": "application/json"})
    if response:
        return filter_for_wikidata_concepts(response.json()["locals"])
    else:
        return []


def get_wikidata_equivalent_for_dbpedia_uri(uri, http):
    dbpedia_query = f"""prefix owl:<http://www.w3.org/2002/07/owl#>
    SELECT DISTINCT ?sameAs WHERE {{
    <{uri}> owl:sameAs ?sameAs 
    FILTER ( strstarts(str(?sameAs), "http://www.wikidata.org/") )
    }}"""
    response = http.get(URL_TO_DBPEDIA_ENDPOINT, params={"query": dbpedia_query},
                        headers={"accept": "application/json"})

    resp = response.json()
    entries = []
    if resp:
        results = resp["results"]["bindings"]
        if len(results) == 0:
            results_from_global = get_wikidata_entry_from_global_thingy(uri, http)
            if len(results_from_global) == 0:
                tqdm.write(f"[WARNING] Couldn't find link for {uri} in either dbpedia or global!")
            entries.extend(results_from_global)
        else:
            entries.extend(item["sameAs"]["value"] for item in results)
    return entries


def _split_ambiguous_from_unambiguous_linkings(data):
    linkings = {}
    ambiguous = []
    unambiguous = []
    for entry in data:
        tmp = linkings.get(entry["db_uri"], [])
        tmp.append(entry["wd_uri"])
        linkings[entry["db_uri"]] = tmp
    for source, targets in linkings.items():
        if len(targets) == 1:
            unambiguous.append({"db_uri": source, "wd_uri": targets[0]})
        else:
            for target in targets:
                ambiguous.append({"db_uri": source, "wd_uri": target, "keep": ""})
    return ambiguous, unambiguous


def split_ambiguous_from_unambiguous_linkings(data):
    ambiguous, _ = _split_ambiguous_from_unambiguous_linkings(data)
    dump_tsv(DBPEDIA_TO_WIKIDATA_AMBIGUOUS, ambiguous)


def make_dbpedia_to_wikidata_dump(data, dump_path):
    http = create_retrying_session()
    links = []
    for item in tqdm(data):
        sameAs_uris = get_wikidata_equivalent_for_dbpedia_uri(item, http)
        for uri in sameAs_uris:
            links.append({"db_uri": item, "wd_uri": uri, "keep": ""})
    split_ambiguous_from_unambiguous_linkings(links)
    dump_tsv(dump_path, links)


def sanity_check_db_wd_linking():
    load = load_tsv(DBPEDIA_TO_WIKIDATA)
    result = True
    expected_fieldnames = {"db_uri", "wd_uri", "keep"}
    actual_fieldnames = set(load[0].keys())
    if actual_fieldnames != expected_fieldnames:
        log(
            f"DBpedia -> Wikidata linking has unexpected fieldnames! Actual: {', '.join(actual_fieldnames)}; Expected: {', '.join(expected_fieldnames)}",
            LogLevel.ERROR)
        result = False
    if "keep" in actual_fieldnames:
        if all(line["keep"].strip() == "" for line in load):
            log(f"DBpedia -> Wikidata linking has no annotation in the `keep` column!", LogLevel.ERROR)
            result = False
    return result


LINKING_MANUAL = f"""You need to do the manual annotation part in {DBPEDIA_TO_WIKIDATA_AMBIGUOUS}. See README.md for instructions."""


def filter_linking_for_applicables_and_merge(force=False):
    disambiguated = {}
    for line in load_tsv(DBPEDIA_TO_WIKIDATA_AMBIGUOUS):
        if len(line["keep"].strip()) > 0:
            disambiguated[line["db_uri"]] = line["wd_uri"]
    ambiguous, unambiguous = _split_ambiguous_from_unambiguous_linkings(load_tsv(DBPEDIA_TO_WIKIDATA))
    already_logged = set()
    for amb in ambiguous:
        if amb["db_uri"] not in disambiguated and amb["db_uri"] not in already_logged:
            if not force:
                log(f"{amb['db_uri']} not disambiguated in {DBPEDIA_TO_WIKIDATA_AMBIGUOUS}! Skipping.", LogLevel.WARNING)
            already_logged.add(amb["db_uri"])
    unambiguous.extend({"db_uri": key, "wd_uri": value} for key, value in disambiguated.items())
    dump_tsv(DBPEDIA_TO_WIKIDATA_INTERNAL, unambiguous)
    encountered_errors = len(already_logged)
    if not force:
        log(f"Encountered {encountered_errors} missing links.", LogLevel.WARNING)
    return encountered_errors


@timer
def link_dbpedia_with_wikidata(graph: HelloWorldExample, force=False):
    query = f""" USING PERIODIC COMMIT 5000
        LOAD CSV WITH HEADERS FROM 'file:///{DBPEDIA_TO_WIKIDATA_INTERNAL}' AS row
        FIELDTERMINATOR "\t"
        MERGE (db:DBConcept {{uri : row.db_uri}})
        MERGE (wd:WDConcept {{uri : row.wd_uri}})
        MERGE (db)-[:owl_sameAs]->(wd)
        MERGE (db)<-[:owl_sameAs]-(wd)
        """
    if not Path(DBPEDIA_TO_WIKIDATA).is_file():
        log(
            f"No link dump found at {DBPEDIA_TO_WIKIDATA}, building now (This will require human intervention later!)..")
        select_query = """MATCH (d:DBConcept)
        RETURN DISTINCT d.uri
        """
        db_uris = [e["d.uri"] for e in graph.select(select_query)]
        make_dbpedia_to_wikidata_dump(db_uris, DBPEDIA_TO_WIKIDATA)
        log("Done.")
        log(LINKING_MANUAL)
        sys.exit(1)
    else:
        encountered_errors = filter_linking_for_applicables_and_merge(force)
        if not encountered_errors or force:
            graph.execute_query_without_transaction(query)
        else:
            log(LINKING_MANUAL)
            sys.exit(1)


def query_wd_for_P31(batch, http):
    baseuri = "https://query.wikidata.org/sparql"
    query = f"""PREFIX wd: <http://www.wikidata.org/prop/direct/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT DISTINCT ?instance ?class 
  WHERE {{
    VALUES (?instance) {{
        {' '.join(f"(<{elem['wd.uri']}>)" for elem in batch)}
    }}
    ?instance wd:P31 ?class.
  }}
"""
    response = http.get(baseuri, params={"query": query},
                        headers={"accept": "application/json"})
    resp = response.json()
    entries = []
    if resp:
        results = resp["results"]["bindings"]
        for res in results:
            entries.append((res["instance"]["value"], res["class"]["value"]))

    return entries


@timer
def _get_classes_for_wd(graph: HelloWorldExample):
    all_uris = graph.select("MATCH (wd:WDConcept) RETURN DISTINCT wd.uri")
    http = create_retrying_session()
    data = []
    batches = [all_uris[i:i + 100] for i in range(0, len(all_uris), 100)]
    for batch in tqdm(batches):
        for instance, clazz in query_wd_for_P31(batch, http):
            data.append({
                "instance": instance, "class": clazz})
    dump_tsv(WD_CLASSES, data)


def get_classes_for_wd(graph: HelloWorldExample):
    if not Path(WD_CLASSES).is_file():
        log(f"{WD_CLASSES} does not exist, creating..")
        _get_classes_for_wd(graph)

    query = f"""
        USING PERIODIC COMMIT 5000
        LOAD CSV WITH HEADERS FROM 'file:///{WD_CLASSES}' AS row
        FIELDTERMINATOR "\t"
        MATCH (a:WDConcept)
        WHERE a.uri = row.instance
        MERGE (b:WDConcept {{uri: row.class}})
        MERGE (a)-[:wd_P31]->(b)
        """
    graph.execute_query_without_transaction(query)


def query_wd_for_P279(batch, http):
    baseuri = "https://query.wikidata.org/sparql"
    query = f"""PREFIX wd: <http://www.wikidata.org/prop/direct/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

    SELECT DISTINCT ?class ?superclass 
      WHERE {{
        VALUES (?class) {{
            {' '.join(f"(<{elem}>)" for elem in batch)}
        }}
        ?class wd:P279 ?superclass.
      }}
    """
    response = http.get(baseuri, params={"query": query},
                        headers={"accept": "application/json"})
    resp = response.json()
    entries = []
    if resp:
        results = resp["results"]["bindings"]
        for res in results:
            entries.append((res["class"]["value"], res["superclass"]["value"]))

    return entries


def _get_hierarchy_for_wd(graph: HelloWorldExample):
    uris_to_query = [uri["class.uri"] for uri in graph.select("MATCH (class:WDConcept) RETURN DISTINCT class.uri")]
    http = create_retrying_session()
    data = []
    depth = 5
    already_queried = set()
    for _ in tqdm(range(depth)):
        tmp = []
        batches = [uris_to_query[i:i + 100] for i in range(0, len(uris_to_query), 100)]
        for batch in tqdm(batches, leave=False):
            for clazz, superclazz in query_wd_for_P279(batch, http):
                already_queried.add(clazz)
                tmp.append({
                    "superclass": superclazz, "class": clazz})
        uris_to_query.clear()
        for new in tmp:
            if new["superclass"] not in already_queried:
                uris_to_query.append(new["superclass"])
            data.append(new)

    dump_tsv(WD_HIERARCHY, data)


@timer
def get_class_hierarchy_for_wd(graph: HelloWorldExample):
    log("Writing wd:P279 relations..")
    if not Path(WD_HIERARCHY).is_file():
        log(f"{WD_HIERARCHY} does not exist, creating..")
        _get_hierarchy_for_wd(graph)
    query = f"""
        USING PERIODIC COMMIT 5000
        LOAD CSV WITH HEADERS FROM 'file:///{WD_HIERARCHY}' AS row
        FIELDTERMINATOR "\t"
        MERGE (class:WDConcept {{uri : row.class}})
        MERGE (super:WDConcept {{uri : row.superclass}})
        MERGE (class)-[:wd_P279]->(super)
        """
    graph.execute_query_without_transaction(query)
    log("Done.")


def query_wd_for_label(batch, http):
    query = f"""PREFIX wd: <http://www.wikidata.org/prop/direct/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

    SELECT DISTINCT ?uri ?uriLabel 
      WHERE {{
        VALUES (?uri) {{
            {' '.join(f"(<{elem['wd.uri']}>)" for elem in batch)}
        }}
    SERVICE wikibase:label {{
      bd:serviceParam wikibase:language "en" .
    }}
      }}
    """
    baseuri = "https://query.wikidata.org/sparql"

    response = http.get(baseuri, params={"query": query},
                        headers={"accept": "application/json"})
    resp = response.json()
    entries = []
    if resp:
        results = resp["results"]["bindings"]
        for res in results:
            entries.append((res["uri"]["value"], res["uriLabel"]["value"]))
    return entries


def _get_label_for_wd(graph: HelloWorldExample):
    query = """MATCH (wd:WDConcept) RETURN DISTINCT wd.uri"""
    all_uris = graph.select(query)
    batches = [all_uris[i:i + 100] for i in range(0, len(all_uris), 100)]
    data = []

    http = create_retrying_session()
    for batch in tqdm(batches):
        for uri, uri_label in query_wd_for_label(batch, http):
            data.append({"uri": uri, "uri_label": uri_label})
    dump_tsv(WD_LABELS, data)


@timer
def get_label_for_wd(graph: HelloWorldExample):
    log("Setting labels to wikidata concepts..")
    if not Path(WD_LABELS).is_file():
        log(f"{WD_LABELS} does not exist, creating..")
        _get_label_for_wd(graph)

    query = f"""
        USING PERIODIC COMMIT 5000
        LOAD CSV WITH HEADERS FROM 'file:///{WD_LABELS}' AS row
        FIELDTERMINATOR "\t"
        MATCH (a:WDConcept)
        WHERE a.uri = row.uri
        SET a.label = row.uri_label
        """
    graph.execute_query_without_transaction(query)
    log("Done.")


http = create_retrying_session()


def extract_bindings_or_empty_list(response: requests.Response) -> Union[List[Dict[str, Any]], List]:
    if not response:
        return []
    else:
        return response.json()["results"]["bindings"]


def get_label_and_uri_from_wikidata_for_label_and_type(label, type):
    query = f"""
                PREFIX wd: <http://www.wikidata.org/prop/direct/>
        SELECT DISTINCT ?uri ?label 
                WHERE {{
                    VALUES(?label) {{
                       ( "{label}" )
                      }}
                    ?uri ?p ?label
                     ; wd:P31/wd:P279* <{type}>

               SERVICE wikibase:label {{
                 bd:serviceParam wikibase:language "en" .
               }}
                }}"""

    r = http.get(WD_SPARQL_ENDPOINT, params={'format': 'json', 'query': query})
    return r


stupid_capitalization = {
    "Of": "of",
    "And": "and",
    "For": "for",
    "The": "the"
}


def make_wikidata_not_cry(string):
    return " ".join(
        stupid_capitalization[word] if word in stupid_capitalization.keys() else word for word in string.split(" "))


def make_entites_csv(entity_file_path, entity_uri, entity_label):
    labels = set()
    from pathlib import Path
    label_mapping = {}
    if Path(entity_file_path).is_file():
        with open(entity_file_path, encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=";")
            for line in reader:
                label = line[entity_label]
                tmp = label_mapping.get(label, [])
                tmp.append(line["uri"])
                label_mapping[label] = tmp
    else:
        log(f"File {entity_file_path} with label mapping is empty.", LogLevel.WARNING)
    with open("../data/speaker.tsv", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for line in reader:
            label = line["country"]
            labels.add(label)
    log(f"Read {len(label_mapping)} from previous run.")
    for label in tqdm(labels):
        if label not in label_mapping.keys():
            label = make_wikidata_not_cry(label)
            response = get_label_and_uri_from_wikidata_for_label_and_type(label, entity_uri)
            result = extract_bindings_or_empty_list(response)
            if len(result):
                for r in result:
                    tmp = label_mapping.get(label, [])
                    tmp.append(r["uri"]["value"])
                    label_mapping[label] = tmp
            else:

                if response.status_code != 200:
                    tqdm.write(f"[WARNING] No {entity_label} result for {label}, reason {response}.")
                else:
                    tqdm.write(f"[INFO] No {entity_label} result for {label}.")
    with open(entity_file_path, "w", encoding="utf-8") as f:
        f.write(f"{entity_label};uri;label\n")
        for label, uris in label_mapping.items():
            for uri in uris:
                f.write(f"{label};{uri};{entity_label}\n")


def annotate_speech_country2(graph):
    country_annotation_file = COUNTRY_MAPPING
    entity_label = "Country"
    uri = "http://www.wikidata.org/entity/Q6256"
    if not Path(country_annotation_file).is_file():
        make_entites_csv(country_annotation_file, uri, entity_label)
    batch_add_from_file_to_db_with_entity_label(graph, country_annotation_file, entity_label)
    log("Done.")


def batch_add_from_file_to_db_with_entity_label(graph, annotation_file, entity_label):
    log(f"Annotating {entity_label} label from {annotation_file}..")
    query = f"""
    LOAD CSV WITH HEADERS FROM 'file:///{annotation_file}' AS row
    FIELDTERMINATOR ";"
    MATCH (e:Institution {{name : row.{entity_label}}})
    SET e:{entity_label}
    MERGE (w:WDConcept {{uri : row.uri}})
    MERGE (w)-[:owl_sameAs]->(e)
    MERGE (w)<-[:owl_sameAs]-(e)
    """
    graph.execute_query(query)
