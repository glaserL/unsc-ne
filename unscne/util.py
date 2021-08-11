import csv
import os
import re
from enum import Enum
from functools import wraps
from pathlib import Path
from random import gauss
from time import sleep
from time import time
from typing import Dict

import requests
import spacy
from requests.adapters import HTTPAdapter
from difflib import ndiff
from requests.packages.urllib3.util.retry import Retry
from tqdm import tqdm

from config import REQUIRED_FILES, CORPUS_TAR, SPEECHES_FOLDER

DEBUG = False


def required_files_are_present() -> bool:
    return all(Path(path).exists() for path in REQUIRED_FILES) or Path(CORPUS_TAR).exists()

def get_total_number_of_speeches():
    x = 0
    with os.scandir(SPEECHES_FOLDER) as root_dir:
        for path in root_dir:
            if path.is_file():
                x += 1
    return x


def count_lines_in_file(path: str):
    with open(path) as f:
        return sum([1 for _ in f])


def generate_speeches_as_tuples():
    i = 0
    with os.scandir(SPEECHES_FOLDER) as root_dir:
        for path in root_dir:
            if path.is_file():
                data = ""
                with open(path, encoding="utf-8") as f:
                    for line in f:
                        data += line.rstrip() + "\n"
                speech_name = ".".join(path.name.split(".")[:-1])
                yield speech_name, data
            i += 1
            if DEBUG and i >= 100:
                break


def get_speech_file_paths(to_list=False):
    result = []
    with os.scandir(SPEECHES_FOLDER) as root_dir:
        for path in root_dir:
            if path.is_file():
                if to_list:
                    result.append(path)
                else:
                    yield path
    if to_list:
        return result


def load_speeches_into_dict() -> Dict[str, str]:
    result = {}
    i = 0
    for path in get_speech_file_paths():
        data = load_file(path)
        speech_name = ".".join(path.name.split(".")[:-1])
        result[speech_name] = data
        i += 1
        if DEBUG and i >= 100:
            break
    return result


def load_file(path):
    data = ""
    with open(path, encoding="utf-8") as f:
        for line in f:
            data += line.rstrip() + "\n"
    return data


DETECT_INITIAL_PHRASES = re.compile(r"(^.*\(.*\):)|The President:\s?")
REGIEANWEISUNGEN = re.compile(r"(\(spoke in (\w*)\))|The President:")


def remove_initial_stub(text):
    return re.sub(DETECT_INITIAL_PHRASES, "", text, 1)


def remove_regieanweisungen(text):
    return re.sub(REGIEANWEISUNGEN, "", text, 1)


def timer(func):
    @wraps(func)
    def _time_it(*args, **kwargs):
        start = int(round(time() * 1000))
        try:
            return func(*args, **kwargs)
        finally:
            end_ = int(round(time() * 1000)) - start
            log(f"Total execution time: {end_ if end_ > 0 else 0} ms")

    return _time_it


def wait(mu, sigma=3.0):
    def decorator(func):
        def wrapper(*args, **kwargs):
            ret = func(*args, **kwargs)
            time_to_sleep = abs(gauss(mu, sigma))
            sleep(time_to_sleep)
            return ret

        return wrapper

    return decorator


def create_retrying_session():
    http = requests.Session()
    retry_strategy = Retry(total=100, backoff_factor=4, status_forcelist=[429, 500, 502, 503, 504],
                           allowed_methods=["HEAD", "GET", "OPTIONS"])
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http.mount("https://", adapter)
    http.mount("http://", adapter)
    return http


class LogLevel(Enum):
    INFO = 0
    WARNING = 1
    ERROR = 2
    PLAIN = 9

import datetime


def log(text: str, level=LogLevel.INFO) -> None:
    now = datetime.datetime.now()
    if level == LogLevel.INFO:
        print(f"[INFO] ({now:%Y-%m-%d %H:%M:%S}) {text}")
    elif level == LogLevel.WARNING:
        print(f"[WARNING] ({now:%Y-%m-%d %H:%M:%S}) {text}")
    elif level == LogLevel.ERROR:
        print(f"[ERROR] ({now:%Y-%m-%d %H:%M:%S}) {text}")
    elif level == LogLevel.PLAIN:
        print(text)
    else:
        print(f"({now:%Y-%m-%d %H:%M:%S}) text")


def dump_tsv(target, data, headers=None, delimiter="\t"):
    if headers is None:
        headers = data[0].keys()
    with open(target, "w", encoding="utf-8") as f:
        writer = csv.DictWriter(f, headers, delimiter=delimiter)
        writer.writeheader()
        for datum in tqdm(data):
            writer.writerow(datum)


def dump_csv(target, data, headers=None):
    dump_tsv(target, data, headers, ";")


def load_csv(source_path, delimiter=";"):
    return load_tsv(source_path, delimiter)


def load_tsv(source_path, encoding="utf-8", delimiter="\t"):
    result = []
    with open(source_path, encoding=encoding) as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        for line in reader:
            result.append(line)
    if len(result) == 0:
        print(f"[WARNING] {source_path} is empty.")
    return result


def write_to_path(data, path):
    with open(path, "w", encoding="utf-8") as f:
        f.write(data)


def get_number_of_files_in_path(root):
    total = 0
    for path in os.scandir(root):
        if path.is_file():
            total += 1
    return total

def levenshtein_distance(str1, str2, ):
    """Stolen from https://codereview.stackexchange.com/questions/217065/calculate-levenshtein-distance-between-two-strings-in-python"""
    counter = {"+": 0, "-": 0}
    distance = 0
    for edit_code, *_ in ndiff(str1, str2):
        if edit_code == " ":
            distance += max(counter.values())
            counter = {"+": 0, "-": 0}
        else:
            counter[edit_code] += 1
    distance += max(counter.values())
    return distance


nlp = spacy.load('en_core_web_sm')


def make_sentence_splitter():
    splitter = spacy.load('en_core_web_sm')
    splitter.disable_pipes('ner')
    return splitter


sentence_splitter = make_sentence_splitter()
