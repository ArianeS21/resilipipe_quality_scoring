from pathlib import Path
import pyarrow as pa
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from resiliparse.parse.html import DOMNode, HTMLTree
from typing import Dict, Sequence, Tuple, Union

from resilipipe.parse.modules.abstract import HTMLModule


class Module(HTMLModule):
    @staticmethod
    def prepare(resources_path: Path):
        pass

    @staticmethod
    def load_input(resources_path: Path) -> Dict:
        module_input = {}
        return module_input

    @staticmethod
    def get_pyarrow_schema() -> Sequence[pa.field]:
        schema = [
            pa.field("outgoing_links", pa.list_(pa.string()), True),
            pa.field("image_links", pa.list_(
                pa.struct([("src", pa.string()), ("width", pa.string()), ("height", pa.string())])
            ), True),
            pa.field("video_links", pa.list_(
                pa.struct([("src", pa.string()), ("width", pa.string()), ("height", pa.string())])
            ), True),
            pa.field("iframes", pa.list_(
                pa.struct([("src", pa.string()), ("width", pa.string()), ("height", pa.string())])
            ), True)
        ]
        return schema

    @staticmethod
    def get_spark_schema() -> Sequence[StructField]:
        schema = [
            StructField("outgoing_links", ArrayType(StringType()), True),
            StructField("image_links", ArrayType(StructType([
                StructField("src", StringType(), True),
                StructField("width", StringType(), True),
                StructField("height", StringType(), True)
            ])), True),
            StructField("video_links", ArrayType(StructType([
                StructField("src", StringType(), True),
                StructField("width", StringType(), True),
                StructField("height", StringType(), True)
            ])), True),
            StructField("iframes", ArrayType(StructType([
                StructField("src", StringType(), True),
                StructField("width", StringType(), True),
                StructField("height", StringType(), True)
            ])), True)
        ]
        return schema

    @staticmethod
    def process_record(tree: HTMLTree, plain_text: str, language: str, json_ld: Sequence, microdata: Sequence, **kwargs) \
            -> Dict[str, Union[str, None]]:
        '''
        Collects all outgoing links from a provided HTMLTree instance.

        :param tree:        The HTMLTree instance in which to look for outgoing links
        :param plain_text:  The plain text as extracted by resiliparse.extract.html2text; Included for compatibility
        :param language:    The language of the HTML record in ISO-639-3 notation; Included for compatibility
        :param json_ld:     List of JSON-LD (https://www.w3.org/TR/json-ld/#embedding-json-ld-in-html-documents);
                            Included for compatibility
        :param microdata:   List of HTML Microdata (http://www.w3.org/TR/microdata/#json); Included for compatibility
        :param kwargs:      Dictionary of content prepared by the "load_input" method; Included for compatibility
        :return:            Dictionary with the following keys:
                            - 'outgoing_links': List of links (str) for nodes with an `a`-tag
                            - 'image_links': List of tuples for nodes with an `img`-tag
                                (src (str), width (str), height (str)); width and height are None if unavailable
                            - 'video_links': List of tuples for nodes with an `video`-tag or videos in an iframe
                                (src (str), width (str), height (str)); width and height are None if unavailable
                            - 'iframes': List of tuples for nodes that contain an iframe (and are not a video)
                                (src (str), width (str), height (str)); width and height are None if unavailable
        '''
        outgoing_links = [node.getattr('href') for node in tree.document.query_selector_all('a[href^=http]')]

        # Get the image and video links; store their width and height if available
        img_links = _get_media_links(tree=tree, selector='img[src^=http]')
        video_links = _get_media_links(tree=tree, selector='video')

        # Identify iframes and additional videos in iframes
        iframes, iframe_videos = _get_iframes(tree=tree)
        video_links += iframe_videos

        return {'outgoing_links': None if len(outgoing_links) == 0 else outgoing_links,
                'image_links': None if len(img_links) == 0 else img_links,
                'video_links': None if len(video_links) == 0 else video_links,
                'iframes': None if len(iframes) == 0 else iframes}


def _get_media_links(tree: HTMLTree, selector: str) -> Sequence[Tuple]:
    media_nodes = [node for node in tree.document.query_selector_all(selector)]
    media_links = []
    for node in media_nodes:
        src = _select_src(node=node)
        width = str(node.getattr('width')) if node.hasattr('width') else None
        height = str(node.getattr('height')) if node.hasattr('height') else None
        if src:
            media_links.append((src, width, height))
    return media_links


def _get_iframes(tree: HTMLTree) -> Tuple[Sequence[Tuple], Sequence[Tuple]]:
    iframe_nodes = [node for node in tree.document.query_selector_all('iframe')]
    iframes, video_links = [], []
    for node in iframe_nodes:
        src = _select_src(node=node)
        width = str(node.getattr('width')) if node.hasattr('width') else None
        height = str(node.getattr('height')) if node.hasattr('height') else None
        if src:
            if _video_in_iframe(src=src):
                video_links.append((src, width, height))
            else:
                iframes.append((src, width, height))
    return iframes, video_links


def _video_in_iframe(src: str):
    file_extensions = [".mp4", ".webm", ".mov", ".avi", ".flv"]
    video_platforms = ["youtube.com", "youtu.be", "vimeo.com", "dailymotion.com", "streamable.com", "videopress.com"]
    url_segments = ["/video/", "/videos/", "watch?"]
    media_service_apis = ["api.video", "cdn.content.video"]

    check_list = [*file_extensions, *video_platforms, *url_segments, *media_service_apis]
    return any([pattern in src for pattern in check_list])


def _select_src(node: DOMNode) -> Union[str, None]:
    if node.hasattr('src'):
        src = node.getattr('src')
    else:
        src_node = node.query_selector('*[src^=http]')
        src = src_node.getattr("src") if src_node else None
    return src
