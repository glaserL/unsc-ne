import os
import sys
import tarfile
from pathlib import Path

from tqdm import tqdm

from config import PARSED_DATA, PARAGRAPH_META, SPEECHES_FOLDER, PARAGRAPHS_PATH, CORPUS_TAR
from unscne.load_meta import split_speech_into_paragraphs, split_into_sentences
from unscne.ner import make_dbpedia_dump
from unscne.util import get_number_of_files_in_path, remove_initial_stub, load_file, dump_tsv, log, LogLevel, write_to_path, \
    required_files_are_present

if not Path("needs_annotation").exists():
    Path("needs_annotation").mkdir()

def main():
    path_to_speeches, path_to_paragraphs, meta_dump_path = SPEECHES_FOLDER, PARAGRAPHS_PATH, PARSED_DATA
    log("Splitting speeches into paragraphs, sentences and removing clutter.")
    Path(path_to_paragraphs).mkdir(parents=True, exist_ok=True)
    indices = []
    paragraph_stuff = []
    with os.scandir(path_to_speeches) as root_dir:
        total = get_number_of_files_in_path(path_to_speeches)
        if total == 0:
            log(f"No files in {path_to_speeches}!", LogLevel.WARNING)
        for path in tqdm(root_dir, total=total):
            if path.is_file():
                speech_name = ".".join(path.name.split(".")[:-1])

                basename = "_".join(speech_name.split("_")[:-1])
                raw_speech = remove_initial_stub(load_file(path))
                for p_index, paragraph in enumerate(split_speech_into_paragraphs(raw_speech)):
                    paragraph_folder = Path(path_to_paragraphs, f"{speech_name}")
                    paragraph_folder.mkdir(parents=True, exist_ok=True)

                    paragraph_id = Path(paragraph_folder, f"{p_index}")
                    paragraph_path = Path(paragraph_folder, f"{p_index}.txt")
                    clean_paragraph = []

                    for s_index, sentence in enumerate(split_into_sentences(paragraph)):
                        sentence = sentence.strip().replace("\t", " ")
                        if len(sentence.strip()):
                            clean_paragraph.append(sentence)
                            s_id = f"{paragraph_id}_{s_index}"
                            indices.append({
                                "speech_name": speech_name,
                                "speech_basename": basename,
                                "paragraph_path": paragraph_path,
                                "p_index": p_index,
                                "s_index": s_index,
                                "s_id": s_id,
                                "p_id": paragraph_id,
                                "text": sentence,
                                "filename": path.name}
                            )

                    write_to_path("\n".join(clean_paragraph), paragraph_path)
                    paragraph_stuff.append({"p_id": paragraph_id, "paragraph_path": paragraph_path})
    dump_tsv(meta_dump_path, indices, list(indices[0].keys()))
    dump_tsv(PARAGRAPH_META, paragraph_stuff)


def count_number_of_files_in_path(path: str) -> int:
    return sum(1 for _ in os.scandir(path))


def unpack_speeches():
    if not Path(CORPUS_TAR).exists():
        log(f"{CORPUS_TAR} does not exist!", LogLevel.ERROR)
    tar = tarfile.open(CORPUS_TAR)
    if not Path(SPEECHES_FOLDER).is_dir():
        log(f"Creating {SPEECHES_FOLDER} to unpack {CORPUS_TAR} into.")
        Path(SPEECHES_FOLDER).mkdir()
    if count_number_of_files_in_path(SPEECHES_FOLDER) < 10:
        log(f"Unpacking {CORPUS_TAR}..")
        tar.extractall(SPEECHES_FOLDER)
        tar.close()
        log("Done")


function_map = {
    "setup": unpack_speeches,
    "parse": main,
    "annotate": make_dbpedia_dump
}

if not required_files_are_present():
    url_to_download_folder = "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KGVSYH"
    log(f"Unpack recent dataset into data/ folder.\nShould be available at {url_to_download_folder}", LogLevel.ERROR)
    sys.exit(1)


unknown = set()
if len(sys.argv) > 1:
    for arg in sys.argv[1:]:
        if arg in function_map.keys():
            func = function_map[arg]
            func()
        else:
            unknown.add(arg)
    if len(unknown) >= 1:
        log(
            f"Available commands: {', '.join(function_map.keys())}\n{len(unknown)} unknown command(s): {', '.join(unknown)}",
            LogLevel.WARNING)
else:
    unpack_speeches()
    main()
    make_dbpedia_dump()
