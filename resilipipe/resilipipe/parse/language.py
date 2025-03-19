from resiliparse.parse.html import HTMLTree
from resiliparse.parse.lang import detect_fast

from iso639 import Language
from typing import Sequence, Union


def process_tree_and_text(tree: HTMLTree, plain_text=None) -> Union[str, None]:
    '''
    Identifies the language of an HTML based on the corresponding HTMLTree.
    This is done in three hierarchical steps.
    1. First node in the document with a lang attribute
    2. Prediction based on Resiliparse's detect_fast on title and description in the HTML's head
    3. Prediction based on Resiliparse's detect_fast on the full plain text

    :param tree:        The HTMLTree instance from which to extract title and description
    :param plain_text:  The plain text as extracted by resiliparse.extract.html2text
    :return:            Standardized language code (ISO 639-3:2007) or None
    '''

    # 1st: Check if the html node has a valid lang attribute
    node = tree.document.query_selector('html[lang]')
    if node is not None:
        try:
            return Language.match(node.getattr('lang')).part3
        except:
            pass

    # 2nd: Look for title and description in the head to use for language classification
    # Use the more confident prediction
    # - detect_fast returns a tuple of (language, out-of-place-measure)
    # - A lower out-of-place-measure indicates higher confidence
    title_desc_list = get_title_desc(tree)
    try:
        prediction_list = sorted([detect_fast(text) for text in title_desc_list], key=lambda x: x[1])
        prediction = prediction_list[0]
    except:
        prediction = ('unknown', None)

    # An out-of-place-measure of above 1200 indicates uncertain classification, use the full text is this case
    if prediction[0] != 'unknown':
        return Language.match(prediction[0]).part3

    # 3rd: Make a prediction on the full plain text of the document (returns 'unknown' if out-of-place-measure of above 1200)
    if plain_text is not None:
        prediction = detect_fast(plain_text)
        if prediction[0] != 'unknown':
            return Language.match(prediction[0]).part3
    return None


def get_title_desc(tree: HTMLTree) -> Sequence[str]:
    '''
    Extract and concatenate the title and description text from a HTMLTree instance.
    Failure to identify str returns empty str.
    :param tree:    The HTMLTree instance from which to extract title and description
    :return:        List of title and description str
    '''
    try:
        title_nodes = tree.head.get_elements_by_tag_name("title")
        title_nodes = tree.body.get_elements_by_tag_name('title') if len(title_nodes) == 0 else title_nodes
        title = title_nodes[0].text
    except:
        title = None

    desc_nodes = tree.head.query_selector_all("[name=description]")
    try:
        desc = desc_nodes[0].getattr('content')
    except:
        desc = None
    return [elem for elem in (title, desc) if elem is not None]

