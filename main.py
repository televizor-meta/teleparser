import glob
import json
from re import finditer

import pymorphy2
from bs4 import BeautifulSoup
from natasha import (
    PER,
    NamesExtractor,
    MorphVocab,
    NewsNERTagger,
    Segmenter,
    NewsEmbedding,
    NewsMorphTagger,
    NewsSyntaxParser,
    Doc
)

emb = NewsEmbedding()
morph_tagger = NewsMorphTagger(emb)
syntax_parser = NewsSyntaxParser(emb)
morph_vocab = MorphVocab()
names_extractor = NamesExtractor(morph_vocab)
segmenter = Segmenter()
ner_tagger = NewsNERTagger(emb)

# не ебу что за парамертр, брал код для parse_names тут
# https://ru.stackoverflow.com/questions/613532/Определение-имени-человека
PROB_THRESH = 0.4

morph = pymorphy2.MorphAnalyzer(lang='ru')


def has_comments(soup: BeautifulSoup):
    return False


def has_names(text: str):
    pass


def parse_names(page_text: str):
    # https://github.com/natasha/natasha
    doc = Doc(page_text)
    doc.segment(segmenter)
    doc.tag_morph(morph_tagger)
    doc.parse_syntax(syntax_parser)
    doc.tag_ner(ner_tagger)
    data = {}
    for span in doc.spans:
        if span.type == PER:
            span.normalize(morph_vocab)
            span.extract_fact(names_extractor)
            if 3 > len(span.tokens) > 1:
                sentence = ('\t', doc.sents[int(span.tokens[0].id.split('_')[0]) - 1].text.strip())
                if not data.get(span.text):
                    data[span.text] = [sentence]
                else:
                    data[span.text].append(sentence)
    return data


def camel_case_split(identifier):
    matches = finditer('.+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)', identifier)
    return [m.group(0) for m in matches]


def split_camel_case(text):
    words = [[text[0]]]

    for c in text[1:]:
        if words[-1][-1].islower() and c.isupper():
            words.append(list(c))
        else:
            words[-1].append(c)

    return [''.join(word) for word in words]


def extract_text(html_page):
    soup = BeautifulSoup(html_page, features="html.parser")
    page_text = soup.find('body').text.strip()
    # remove empty lines
    page_text = '. '.join([i.strip() for i in page_text.split('\n') if i.strip()])

    # split camel cases
    page_text = '. '.join([i.strip() for i in split_camel_case(page_text)])

    return page_text


def process_page(page_path: str):
    with open(page_path, 'r') as file:
        try:
            page_text = extract_text(file)
        except UnicodeDecodeError:
            return []
        return parse_names(page_text)


if __name__ == '__main__':

    path = 'pages/*'
    all_html_files = [f for f in glob.glob(path) if '.html' in f]

    all_pages_data = {}
    for i in range(100):
        all_pages_data[all_html_files[i]] = process_page(all_html_files[i])

    print(json.dumps(all_pages_data, ensure_ascii=False))
