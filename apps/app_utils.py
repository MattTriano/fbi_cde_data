from collections import Counter
import re
from typing import Union

import streamlit as st


def match_sql_case(query: str, keyword: str) -> str:
    keywords = re.findall(r"\b(SELECT|FROM|WHERE|ORDER|GROUP|LIMIT)\b", query, re.IGNORECASE)

    if not keywords:
        return keyword.lower()

    def check_case(word: str) -> str:
        if word.isupper():
            return "upper"
        elif word.istitle():
            return "title"
        else:
            return "lower"

    cases = [check_case(kw) for kw in keywords]
    case_counts = Counter(cases)
    modal_case_count = case_counts.most_common(1)[0][1]
    modal_cases = [case for case, count in case_counts.items() if count == modal_case_count]
    if "lower" not in modal_cases:
        if "upper" in modal_cases:
            return keyword.upper()
        else:
            return keyword.title()
    else:
        return keyword.lower()


def has_unescaped_newline(query: str) -> bool:
    in_string = False
    escape = False
    for char in query:
        if char == "\\" and not escape:
            escape = True
        elif char in ("'", '"') and not escape:
            in_string = not in_string
        elif char == "\n" and not in_string:
            return True
        else:
            escape = False
    return False


def enforce_query_limit(query: str, max_rows: int) -> str:
    limit_keyword = match_sql_case(query, "limit")
    is_multiline = has_unescaped_newline(query)

    limit_pattern = re.compile(r"limit\s+(\d+)", re.IGNORECASE)
    already_limited = limit_pattern.search(query)
    if already_limited:
        current_limit = int(already_limited.group(1))
        if current_limit > max_rows:
            query = limit_pattern.sub(f"{limit_keyword} {max_rows}", query)
        else:
            query = limit_pattern.sub(f"{limit_keyword} {current_limit}", query)
    else:
        query = f"{query}{'\n' if is_multiline else ' '}{limit_keyword} {max_rows}"
    return query


def display_query(query: Union[list[str], str]):
    if isinstance(query, list):
        st.code("\n".join(query), language="sql")
    elif isinstance(query, str):
        st.code(query, language="sql")
    else:
        raise TypeError(f"The query must be provided as a str or list of strs. Received {query}")
