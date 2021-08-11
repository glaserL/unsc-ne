# Paths to places where the pipeline expects files / folders to be on the local drive

# files
CORPUS_TAR = "data/speeches.tar"
META = "data/meta.tsv"
SPEAKER = "data/speaker.tsv"
REQUIRED_FILES = [SPEAKER, META]
COUNTRY_MAPPING = "data/country_to_wd.csv"
DBPEDIA_TO_WIKIDATA = "data/db_to_wd_linking.tsv"
DBPEDIA_TO_WIKIDATA_INTERNAL = "data/db_to_wd_linking_tmp.tsv"
DBPEDIA_TO_WIKIDATA_AMBIGUOUS = "needs_annotation/db_to_wd_linking.tsv"
WD_CLASSES = "data/classes_wd.tsv"
DBPEDIA_NERS = "data/ners.tsv"
SPEECHES_FOLDER = "data/speeches/"
PARAGRAPHS_PATH = "data/paragraphs/"
PARSED_DATA = "data/main.tsv"
PARAGRAPH_META = "data/paragraph_meta.tsv"
WD_LABELS = "data/labels_wd.tsv"
WD_HIERARCHY = "data/hierarchy_wd.tsv"
# dbpedia
URL_TO_DBPEDIA_SERVICE = "http://192.168.178.28:2222/rest/annotate"
URL_TO_DBPEDIA_ENDPOINT = "https://dbpedia.org/sparql"
# wikidata
WD_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
WD_GFS_ENDPOINT = "https://global.dbpedia.org/"
# neo4j settings
NEO4J_BOLT_URL = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "12345"
NEO4J_DATABASE_NAME = "unscne"
