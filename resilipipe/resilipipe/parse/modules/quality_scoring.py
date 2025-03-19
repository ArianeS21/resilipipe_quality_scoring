import math
from pathlib import Path

import pandas as pd
import pyarrow as pa
from pyspark.sql.types import StructField, StructType, FloatType
from resiliparse.parse.html import DOMNode, HTMLTree
from typing import Dict, Sequence, Tuple, Union
from pyterrier_quality import QualT5

from resilipipe.parse.modules.abstract import HTMLModule


class Module(HTMLModule):
    @staticmethod
    def prepare(resources_path: Path):
        print('Preparing quality scoring')

    @staticmethod
    def load_input(resources_path: Path) -> Dict:
        qmodel = QualT5('pyterrier-quality/qt5-small')
        module_input = {"qmodel":qmodel}
        return module_input

    @staticmethod
    def get_pyarrow_schema() -> Sequence[pa.field]:
        schema = [
            pa.field("quality_score", pa.float64(), True)
        ]
        return schema

    @staticmethod
    def get_spark_schema() -> Sequence[StructField]:
        schema = [
            StructField("quality_score", FloatType(), True)
        ]
        return schema

    @staticmethod
    def process_record(tree: HTMLTree, plain_text: str, language: str, json_ld: Sequence, microdata: Sequence, **kwargs) \
            -> Dict[str, float]:
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
                            - 'quality_score': Quality score of the document
        '''

        qmodel = kwargs["qmodel"]
        df = pd.DataFrame({'docno':0,'text':plain_text}, index=[0])
        scored = qmodel.transform(df)
        quality_score = scored['quality'][0]
        return {'quality_score': quality_score if not math.isnan(quality_score) else None}


