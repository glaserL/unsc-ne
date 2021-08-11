import re
from typing import List

from tqdm import tqdm

import config
from unscne.graph import HelloWorldExample
import csv

from unscne.util import timer, log, sentence_splitter, count_lines_in_file, dump_tsv


@timer
def load_metadata_into_graph(graph: HelloWorldExample):
    add_meta_query = f"""
    USING PERIODIC COMMIT 1000
    LOAD CSV WITH HEADERS FROM 'file:///{config.META}' AS row
    FIELDTERMINATOR "\t"
    CREATE (n:Meta {{basename: row.basename, date: row.date, num_speeches: row.num_speeches, topic: row.topic,
pressrelease: row.pressrelease, outcome: row.outcome, year: row.year, month: row.month, day : row.day}})
    """
    log("Loading metadata..")
    graph.execute_query_without_transaction(add_meta_query)
    log("Done.")


def get_fieldnames_in_file(file, delimiter="\t"):
    fieldnames = None
    with open(file, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        fieldnames = reader.fieldnames
    return fieldnames


def link_meta_to_speeches(graph):
    log("Linking speeches to their Metadata..")
    query = """
    MATCH (s:Speech), (m:Meta)
    WHERE s.basename = m.basename
    CREATE (s)-[:HAS_METADATA]->(m)
    """
    graph.execute_query(query)
    log("Done.")


@timer
def create_next_speech_relation(graph):
    log("Creating NEXT relation for speeches..")
    query = """
    MATCH (s1:Speech)-[:HAS_METADATA]->(m:Meta)<-[:HAS_METADATA]-(s2:Speech)
    WHERE toInteger(s1.index) = toInteger(s2.index)-1
    CREATE (s1)-[:NEXT]->(s2)
    """
    graph.execute_query(query)
    log("Done.")


@timer
def create_next_paragraph_relation(graph: HelloWorldExample):
    log("Creating NEXT relation for paragraphs..")
    query = """
    MATCH (p2:Paragraph)<-[:CONTAINS]-(s:Speech)-[:CONTAINS]->(p1:Paragraph)
    WHERE toInteger(p1.index) = toInteger(p2.index)-1
    CREATE (p1)-[:NEXT]->(p2)
    """
    graph.execute_query(query)
    log("Done.")


@timer
def create_next_sentence_relation(graph: HelloWorldExample):
    log("Creating NEXT relation for sentences..")
    query = """
    MATCH (s1:Sentence)<-[:CONTAINS]-(p:Paragraph)-[:CONTAINS]->(s2:Sentence)
    WHERE toInteger(s1.index) = toInteger(s2.index)-1
    CREATE (s1)-[:NEXT]->(s2)
    """
    graph.execute_query_without_transaction(query)
    log("Done.")


def split_speech_into_paragraphs(speech: str) -> List[str]:
    return [f"{p}." for p in re.split(r"\.\n{2,}", speech)]


def split_into_sentences(speech: str) -> List[str]:
    nice_speech = re.sub(r"\n+", " ", speech)
    nlped = sentence_splitter(nice_speech)
    for sent in nlped.sents:
        yield sent.text


def add_president_label(graph: HelloWorldExample):
    log("Adding president label..")
    query = """
    MATCH (s:Speaker {participanttype:"The President"})
    SET s :President"""
    graph.execute_query(query)
    log("Done.")


@timer
def link_paragraph_and_sentence_to_speakers(graph: HelloWorldExample):
    query = """
    MATCH (speak:Speaker)-[:SPOKE]->(speech:Speech)-[:CONTAINS]->(p:Paragraph)-[:CONTAINS]->(s:Sentence) 
    MERGE (speak)-[:SPOKE]->(p)
    MERGE (speak)-[:SPOKE]->(s)
    """
    graph.execute_query(query)


@timer
def load_sentences_into_graph(graph: HelloWorldExample, file_path=config.PARSED_DATA):
    log("Loading sentences..")
    statement = """
        USING PERIODIC COMMIT 5000
        LOAD CSV WITH HEADERS FROM 'file:///%s' AS row
        FIELDTERMINATOR "\t"
        CREATE (s:Sentence {index: toInteger(row.s_index), index_in_speech: toInteger(row.s_index_in_speech),
                            id: row.s_id, text: row.text})
        MERGE (p:Paragraph {index: toInteger(row.p_index), id: row.p_id})
        MERGE (sp:Speech {basename : row.speech_basename, id: row.speech_name, filename: row.filename })
        MERGE (sp)-[:CONTAINS]->(p)
        MERGE (sp)-[:CONTAINS]->(s)
        MERGE (p)-[:CONTAINS]->(s)
        """ % file_path

    graph.execute_query_without_transaction(statement)
    log("Done.")

@timer
def add_speech_meta_to_nodes(graph: HelloWorldExample):
    log("Adding speech meta data..")
    statement = f"""
        USING PERIODIC COMMIT 5000
        LOAD CSV WITH HEADERS FROM 'file:///{config.SPEAKER}' AS row
        FIELDTERMINATOR "\t"
        MATCH (speech:Speech) WHERE speech.filename = row.filename
        MATCH (speech)-[:CONTAINS]->(p:Paragraph)
        MATCH (p)-[:CONTAINS]->(s:Sentence)
        MERGE (speaker:Speaker {{name: row.speaker, participanttype: row.participanttype, role_in_un : coalesce(row.role_in_un, 'N/A'), country: row.country}})
        MERGE (a1:AgendaItem {{name: row.agenda_item1}})
        MERGE (speech)-[:AGENDA1]->(a1)
        MERGE (a2:AgendaItem {{name: row.agenda_item2}})
        MERGE (speech)-[:AGENDA2]->(a2)
        MERGE (a3:AgendaItem {{name: row.agenda_item3}})
        MERGE (speech)-[:AGENDA3]->(a3)
        MERGE (i:Institution {{name: row.country}})
        MERGE (speaker)-[:SPOKE]->(speech)
        MERGE (speaker)-[:REPRESENTS]->(i)
    """
    graph.execute_query_without_transaction(statement)
    log("Done.")


@timer
def unify_agenda_relation(graph: HelloWorldExample):
    statements = ["MATCH (s:Speech)-[:AGENDA1]->(a:AgendaItem) MERGE (s)-[:AGENDA]->(a)",
                  "MATCH (s:Speech)-[:AGENDA2]->(a:AgendaItem) MERGE (s)-[:AGENDA]->(a)",
                  "MATCH (s:Speech)-[:AGENDA3]->(a:AgendaItem) MERGE (s)-[:AGENDA]->(a)"]
    for s in statements:
        graph.execute_query(s)


def get_sentence_and_line_number_by_offset(path, offset):
    offset = int(offset)
    current = 0
    with open(path, encoding="utf-8") as f:
        for i, line in enumerate(f):
            if current + len(line) >= offset:
                new_offset = offset - current
                return i, new_offset
            current += len(line)


def inject_sids_from_pids(file_path: str):
    data = []
    total = count_lines_in_file(file_path)
    with open(file_path, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for i, line in enumerate(tqdm(reader, total=total)):
            line_number, new_offset = get_sentence_and_line_number_by_offset(line['paragraph_path'], line['offset'])
            s_id = f"{line['p_id']}_{line_number}"
            line["s_id"] = s_id
            line["offset"] = new_offset
            data.append(line)
    dump_tsv(file_path, data)
