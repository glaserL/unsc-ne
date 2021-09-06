This repository contains all build scripts to create the Named Entity Corpus Addon to the [UN Security Council Debates corpus](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KGVSYH).

## Requirements
* Required python packages are enumerated in `requirements.txt`.
* It is also highly recommended to have a dedicated instance of DBpedia spotlight running, as the online demo has restrictive usage limits.
    * Set the `annotate/` endpoint in `constants.py`
* We found a simple docker instance to work well, a few pointers on how to get it running:
    * [DBpedia-spotlight docker on GitHub](https://github.com/dbpedia-spotlight/spotlight-docker)
    * [DBpedia-spotlight on Dockerhub](https://hub.docker.com/r/dbpedia/dbpedia-spotlight)
    * [Dbpedia-spotlight API usage limit](https://forum.dbpedia.org/t/dbpedia-spotlight-api-usage-limit/586/21)

## Usage
### make
* Run `python make.py` once. This runs all necessary annotations through spacy / dbpedia.
* This should take quite some time, but needs to be run only once.

### build
* After `make.py` succeeded, the necessary annotations are available and the corpus can be build.
* Make sure neo4j and dbpedia-spotlight are running. Edit `config.py` to change ip addresses, filenames etc.
* Run `python build.py`
* Use `python wipe_db.py` to wipe the entire database if something goes wrong.

### annotate
* The creation of the UNSC-NE corpus addon requires some human input, which has to take place in the third phase.

#### Consolidate DBpedia -> Wikidata links
* `needs_annotation/db_to_wd_linking.tsv` contains the found links from DBpedia to Wikidata. These are an ambiguous 1:m mapping, which needs to be fixed into a 1:1 mapping.
* The column `db_uri` contains the URI to the node in DBpedia, the column `wd_uri` contains the URI to the node in Wikidata.
* Please write anything (not whitespace) in the column `keep` of the relation that you deem correct.

### finalize
* After finishing the manual annotations, you may use `python finalize.py` to finish the corpus.
* If you were unable to consolidate the links for some cases you can use the `-force` argument, causing the still ambiguous links to be skipped.

## Node types and relations

### Nodes 
- Country
  - name: the name of the country
- DBConcept
  - uri: the DBpedia uri this node represents
- Institution
  - name: the name of the institution
- Meta *Represents an entry in meta.tsv of the fundamental UN Security Council debates corpus*
- Paragraph
  - index: the index within the speech it's contained in
- Sentence
  - index_in_speech: the index within the speech it's contained in
  - index: the index within the paragraph it's contained in
  - text: the text of the sentence itself
- Speaker *Represents an entry in speaker.tsv of the fundamental UN Security Council debates corpus*
- Speech
- AgendaItem
  - name: the name of the agenda item
- WDConcept
  - uri: the Wikidata URI this node represents
  - label: the English string label of this node
  
### Relationships
  
- CONTAINS:
  - Speech -> Paragraph
  - Speech -> Sentence
  - Paragraph -> Sentence
- MENTIONS
  - Sentence -> DBConcept
  - surfaceForm: the string that has been annotated
  - offset: the character offset within the sentence
- REPRESENTS
  - Speaker -> Institution
  - Speaker -> Country
- HAS\_METADATA
  - Speech -> Meta
- SPOKE
  - Speaker -> Speech
  - Speaker -> Paragraph
  - Speaker -> Sentence
- AGENDA
  - Speech -> AgendaItem
- owl\_sameAs: links a URI in the DBpedia knowledge graph to a URI in the wikidata knowledge graph it corresponds to
  - DBConcept <-> WDConcept
- wd\_P279: points from a class to a superclass
  - WDConcept -> WDConcept
- wd\_P31: points from an instance to a class
  - WDConcept -> WDConcept
- NEXT
  - Sentence -> Sentence
  - Speech -> Speech
  - Paragraph -> Paragraph

## Known Issues
* *Export of jsonlines does not work* Make sure apoc.export.file.enabled=true in the neo4j settings.
* *Import files can't be found* dbms.directories.import should point to the root directory
