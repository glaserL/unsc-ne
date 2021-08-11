import math
import sys
from typing import Optional

import neo4j
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
from tqdm import tqdm

from config import NEO4J_PASSWORD, NEO4J_USER, NEO4J_BOLT_URL, NEO4J_DATABASE_NAME
from unscne.util import log, LogLevel


class HelloWorldExample:

    def __init__(self, uri, user, password, database_name = NEO4J_DATABASE_NAME):
        self._create_database_if_not_exists(uri, user, password)
        self.driver = GraphDatabase.driver(uri, database=database_name, auth=(user, password))


    def _create_database(self, uri, user, password, database_name):
        driver = GraphDatabase.driver(uri, database="system", auth=(user, password))
        with driver.session() as session:
            print(f"Creating {database_name}")
            session.run(f"CREATE DB {database_name}")
        driver.close()

    def _create_database_if_not_exists(self, uri, user, password):
        tmp = GraphDatabase.driver(uri, database="system", auth=(user, password))
        with tmp.session() as session:
            session.run(f"CREATE DATABASE {NEO4J_DATABASE_NAME} IF NOT EXISTS")

    def close(self):
        self.driver.close()

    @staticmethod
    def _get_number_of_nodes_in_graph(tx):
        result = tx.run("MATCH (n) RETURN COUNT(n)").single()
        return result["COUNT(n)"]

    def get_number_of_nodes_in_graph(self):
        with self.driver.session() as session:
            return session.read_transaction(self._get_number_of_nodes_in_graph)

    def clear_indices(self):
        query = "CALL db.indexes"
        log("Clearing indexes..")
        for constraint in self.select(query):
            self.execute_query(f"DROP INDEX {constraint['name']}")
        log("Done.")

    def clear_constraints(self):
        query = "CALL db.constraints"
        log("Clearing constraints..")
        for constraint in tqdm(self.select(query)):
            self.execute_query(f"DROP CONSTRAINT {constraint['name']}")
        log("Done.")

    def clear(self):
        self.clear_data()
        self.clear_constraints()
        self.clear_indices()

    def clear_data(self):
        batch_number = 10000
        query = f"""
        MATCH (n)
        WITH n LIMIT {batch_number}
        DETACH DELETE n;  
        """
        log("Clearing data..")
        for _ in tqdm(range(math.ceil(self.get_number_of_nodes_in_graph() / batch_number))):
            self.execute_query(query)
        log("Done.")

    @staticmethod
    def _create_and_return_greeting(tx, source, target):
        result = tx.run("CREATE (a:Greeting) "
                        "SET a.message = $message "
                        "RETURN a.message + ', from node ' + id(a)", source=source, target=target)
        return result.single()[0]

    def add_meta(self, basename, date, num_speeches, topic, pressrelease, outcome):
        with self.driver.session() as session:
            session.write_transaction(self._write_meta, basename, date, num_speeches, topic, pressrelease, outcome)

    @staticmethod
    def _write_meta(tx, basename, date, num_speeches, topic, pressrelease, outcome):
        query = """
        CREATE (n:Meta {basename: $basename, date: $date, num_speeches: $num_speeches, topic: $topic,
                    pressrelease: $pressrelease, outcome: $outcome})
        """
        tx.run(query, basename=basename, date=date, num_speeches=num_speeches, topic=topic, pressrelease=pressrelease,
               outcome=outcome)

    @staticmethod
    def _add_speech(tx, speech, country, speaker, participanttype, role_in_un):
        query = """
        CREATE (n:Speech {id: $speech, country: $country, speaker: $speaker,
                    participanttype: $participanttype, role_in_un: $role_in_un})
        """
        tx.run(query, speech=speech, country=country, speaker=speaker, participanttype=participanttype,
               role_in_un=role_in_un)

    def add_speech(self, speech, country, speaker, participanttype, role_in_un):
        with self.driver.session() as session:
            session.write_transaction(self._add_speech, speech, country, speaker, participanttype, role_in_un)

    def select(self, query):
        with self.driver.session() as session:
            return session.read_transaction(self._select, query)

    @staticmethod
    def _select(tx, query):
        data = []
        for result in tx.run(query):
            data.append(result)
        return data

    @staticmethod
    def _merge_speech_text(tx, basename, text):
        query = """
        MERGE (s:Speech {id: $basename})
        ON CREATE SET s.id = $basename, s.text_raw = $text
        ON MATCH SET s.text_raw = $text
        """
        tx.run(query, basename=basename, text=text)

    def add_speech_text(self, basename, text):
        with self.driver.session() as session:
            session.write_transaction(self._merge_speech_text, basename, text)

    def execute_query(self, query):
        with self.driver.session() as session:
            return session.write_transaction(lambda tx: tx.run(query))

    def execute_query_and_ignore_exceptions(self, query):
        try:
            self.execute_query(query)
        except neo4j.exceptions.ClientError:
            pass

    def execute_query_without_transaction(self, query):
        with self.driver.session() as session:
            return session.run(query)

    def create_indices_and_constraints(self):
        print("Creating indexes and constraints..")
        self.create_constraints_if_they_dont_exist()
        print("DONE")

    def create_constraints_if_they_dont_exist(self):
        constraints = ["CREATE CONSTRAINT constraint_speech_id IF NOT EXISTS ON (s:Speech) ASSERT (s.filename) IS NODE KEY",
                       "CREATE CONSTRAINT constraint_agenda_name_name IF NOT EXISTS ON (a:AgendaItem) ASSERT (a.name) IS NODE KEY",
                       "CREATE CONSTRAINT constraint_paragraph_index IF NOT EXISTS ON (p:Paragraph) ASSERT (p.id) IS NODE KEY",
                       "CREATE CONSTRAINT constraint_db_uri IF NOT EXISTS ON (d:DBConcept) ASSERT (d.uri) IS NODE KEY",
                       "CREATE CONSTRAINT constraint_wd_uri IF NOT EXISTS ON (w:WDConcept) ASSERT (w.uri) IS NODE KEY",
                       "CREATE CONSTRAINT constraint_sentence_id IF NOT EXISTS ON (s:Sentence) ASSERT (s.id) IS NODE KEY",
                       "CREATE CONSTRAINT constraint_institution_name IF NOT EXISTS ON (i:Institution) ASSERT (i.name) IS NODE KEY",
                       "CREATE CONSTRAINT constraint_speaker IF NOT EXISTS ON (s:Speaker) ASSERT (s.name, s.participanttype, s.role_in_un, s.country) IS NODE KEY"
                       ]
        for constraint in constraints:
            self.execute_query(constraint)

    def create_indices_if_they_dont_exist(self):
        indices = [
            "CREATE INDEX index_meta_basename IF NOT EXISTS FOR (m:Meta) ON m.basename"]
        for index in indices:
            self.execute_query(index)

    def make_index(self):
        query = """
        MATCH (s:Speech)
        SET s.index = toInteger(split(s.id, "spch")[1])
        """
        self.execute_query(query)

    @staticmethod
    def _create_or_update_mention(tx, source, target):
        query = """
        MERGE (A:Country {name: $source})
        MERGE (B:Country {name: $target})
        MERGE (A)-[r:MENTIONED]->(B)
        ON CREATE SET r.weight = 1
        ON MATCH  SET r.weight = r.weight + 1
        """
        tx.run(query, source=source, target=target)

    @staticmethod
    def generate_batches(data, batch_size, batch_desc="BATCH"):
        for i in tqdm(range(0, len(data), batch_size), desc=batch_desc):
            yield data[i:i + batch_size]

    def add_batch(self, query, data, batch_size=1000):
        for batch in self.generate_batches(data, batch_size):
            with self.driver.session() as session:
                tx = session.begin_transaction()
                for params in batch:
                    tx.run(query, params)
                tx.commit()
                tx.close()

    def add_basename_to_speech_batch(self, data):
        query = """
        MERGE (s:Speech {id : $s_id})
        SET s.basename = $basename
        """
        self.add_batch(query, data)


def connect_graph() -> Optional[HelloWorldExample]:
    try:
        return HelloWorldExample(NEO4J_BOLT_URL, NEO4J_USER, NEO4J_PASSWORD)
    except ServiceUnavailable:
        log(f"Please start neo4j under {NEO4J_BOLT_URL}", LogLevel.ERROR)
        sys.exit(1)

