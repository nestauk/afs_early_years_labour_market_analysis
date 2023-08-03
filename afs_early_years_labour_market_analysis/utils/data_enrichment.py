"""
Variables and functions for data enrichment flow
"""
from typing import Union
import spacy
from spacy.matcher import Matcher
import re
import pandas as pd


level_dict = {
    "qts": "6",
    "qtse": "6",
    "eyts": "6",
    "eyps": "6",
    "qualified teacher status": "6",
    "qualified teachers status": "6",
    "early years teacher status": "6",
    "early years professional status": "6",
    "ba": "6",
    "cert": "4",
    "pgce": "7",
    "degree": "6",
    "foundation": "5",
    "foundation degree": "5",
    "qtls": "5",
    "eye": "3",
    "nneb": "3",
}

london_nuts_3 = [
    "UKI31",
    "UKI32",
    "UKI33",
    "UKI34",
    "UKI41",
    "UKI42",
    "UKI43",
    "UKI44",
    "UKI45",
    "UKI51",
    "UKI52",
    "UKI53",
    "UKI54",
    "UKI61",
    "UKI62",
    "UKI63",
    "UKI71",
    "UKI72",
    "UKI73",
    "UKI74",
    "UKI75",
]

patterns = [
    [{"LOWER": "level"}, {"IS_DIGIT": True}],
    [{"LOWER": "l"}, {"IS_DIGIT": True}],
    [{"LOWER": "levels"}, {"IS_DIGIT": True}],
    [{"LOWER": "levels"}, {"IS_DIGIT": True}, {"IS_PUNCT": True}, {"IS_DIGIT": True}],
    [{"LOWER": "level"}, {"POS": "CCONJ"}, {"IS_DIGIT": True}],
    [{"LOWER": "level"}, {"IS_DIGIT": True}, {"POS": "CCONJ"}, {"IS_DIGIT": True}],
    [{"LOWER": "cache"}, {"IS_DIGIT": True}],
    [{"LOWER": "cache"}, {"IS_DIGIT": True}, {"POS": "CCONJ"}, {"IS_DIGIT": True}],
    [{"LOWER": "nvq"}, {"IS_DIGIT": True}, {"POS": "CCONJ"}, {"IS_DIGIT": True}],
    [{"LOWER": "nvq"}, {"IS_DIGIT": True}],
    [{"LOWER": "ba"}, {"IS_SPACE": True}],
    [{"LOWER": "cert"}, {"IS_SPACE": True}],
    [{"LOWER": "degree"}],
    [{"LOWER": "qts"}],
    [{"LOWER": "qtse"}],
    [{"LOWER": "eyts"}],
    [{"LOWER": "eyps"}],
    [{"LOWER": "pgce"}],
    [{"LOWER": "foundation degree"}],
    [{"LOWER": "qualified teacher status"}],
    [{"LOWER": "qualified teachers status"}],
    [{"LOWER": "early years teacher status"}],
    [{"LOWER": "early years professional status"}],
    [{"LOWER": "nneb"}],
    [{"LOWER": "eye"}],
    [{"LOWER": "qtls"}],
]

nlp = spacy.load("en_core_web_sm")
matcher = Matcher(nlp.vocab)
matcher.add("qualification", patterns)


def get_qualification_level(job_description: str) -> Union[int, None]:
    """
    Function to extract qualification levels from a job description.

    Args:
        job_description (str): job description to extract qualification levels from.

    Returns:
        int: minimum qualification level mentioned in job description.
    """
    doc = nlp(job_description)
    matches = matcher(doc)

    qualification_level = []
    for match_id, start, end in matches:
        span = doc[start:end]  # The matched span
        span_text = span.text.lower()
        span_text_number = level_dict.get(span_text, span_text)
        # regex match numbers from span text
        numbers = re.findall(r"\d+", " ".join(span_text_number))
        qualification_level.extend(numbers)

    if qualification_level != []:
        # return the minimum label
        return min([int(level) for level in qualification_level])
    else:
        return None
