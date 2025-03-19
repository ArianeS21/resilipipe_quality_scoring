import gzip
from io import BytesIO
import os
import json
from pandas import DataFrame, read_csv
from pathlib import Path
import pyarrow as pa
from pyspark.sql.types import ArrayType, StringType, StructField
from typing import Dict, Sequence, Union
from urllib.request import urlopen
from resilipipe.parse.modules.abstract import BasicModule


class Module(BasicModule):
    @staticmethod
    def prepare(resources_path: Path):
        json_path = resources_path / 'curlielabels.json.gz'
        if os.path.isfile(json_path):
            print(f'\nThere is already a file at {json_path}. Please delete the existing file '
                  f'if you want to download a new version of the domain labels.\n')
            return

        # Get the original data as a DataFrame
        df = _get_domain_df()

        # Turn it into a dictionary with each domain as key and saved it as a gzipped JSON
        dict_ = {x["domain_url"]: {
            "curlielabels": x["curlielabels"],
            "curlielabels_en": x["curlielabels_en"],
            "lang": x["lang"]
        } for x in df[["domain_url", "curlielabels", "curlielabels_en", "lang"]].to_dict(orient="records")}

        with gzip.open(json_path, 'w') as f:
            f.write(json.dumps(dict_).encode('utf-8'))
        print(f"Saved Curlie labels to {json_path}")

    @staticmethod
    def load_input(resources_path: Path) -> Dict:
        with gzip.open(resources_path / 'curlielabels.json.gz', 'r') as f:
            return {"curlie_dict": json.loads(f.read().decode('utf-8'))}

    @staticmethod
    def get_pyarrow_schema() -> Sequence[pa.field]:
        schema = [
            pa.field("curlielabels", pa.list_(pa.string()), True),
            pa.field("curlielabels_en", pa.list_(pa.string()), True)
        ]
        return schema

    @staticmethod
    def get_spark_schema() -> Sequence[StructField]:
        schema = [
            StructField("curlielabels", ArrayType(StringType()), True),
            StructField("curlielabels_en", ArrayType(StringType()), True),
        ]
        return schema

    @staticmethod
    def process_record(record_dict, **kwargs) -> Dict[str, Union[str, None]]:
        '''
        Take a dictionary of information about a WARC record and add the domain labels to it.
        The dictionary must contain the following keys: 'url_domain' and 'url_suffix'.
        :param record_dict:     Dictionary with record information
        :return:                Updated dictionary with curlie labels
        '''
        curlie_dict = kwargs["curlie_dict"]
        try:
            domain_url = record_dict['url_domain'] + '.' + record_dict['url_suffix']
        except:
            domain_url = None

        results = {'curlielabels': None, 'curlielabels_en': None}
        domain_info = curlie_dict.get(domain_url, None)
        if domain_info:
            for col in results.keys():
                results[col] = domain_info[col]

        return results


def _get_curlie_df(csv_url: str = 'https://figshare.com/ndownloader/files/38937971') -> DataFrame:
    '''
    Downloads a CSV of domain labels scraped from Curlie.org and published by Lugeon, Sylvain; Piccardi, Tiziano.
    Columns of the original DataFrame are url, uid, label, lang

    Source:
    Lugeon, Sylvain; Piccardi, Tiziano (2022).
    Curlie Dataset - Language-agnostic Website Embedding and Classification.
    figshare. Dataset. https://doi.org/10.6084/m9.figshare.19406693.v5

    :param csv_url:     URL from which to download the gzipped CSV file
    :return:            CSV read into a pandas DataFrame
    '''
    resp = urlopen(csv_url)
    with gzip.open(BytesIO(resp.read()), 'rb') as f:
        df = read_csv(f)
    return df.rename(columns={'label': 'domain_label'})


def _get_mapping_dict(json_url: str = 'https://figshare.com/ndownloader/files/34491215') -> Dict[str, str]:
    '''
    Downloads as JSON file that maps from language specific categories of Curlie labels to the english equivalent.
    Prepared by Lugeon, Sylvain; Piccardi, Tiziano.

    Source:
    Lugeon, Sylvain; Piccardi, Tiziano (2022).
    Curlie Dataset - Language-agnostic Website Embedding and Classification.
    figshare. Dataset. https://doi.org/10.6084/m9.figshare.19406693.v5

    :param json_url:    URL from which to download the gzipped CSV file
    :return:            Dictionary that maps from the language specific label to the english equivalent
    '''
    resp = urlopen(json_url)
    with gzip.open(BytesIO(resp.read()), 'rb') as f:
        json_list = list(f)
    return {
        v.replace('\\', ''): x['english_label'].replace('\\', '')
        for x in [eval(mapping) for mapping in json_list]
        for v in x['matchings'].values()
    }


def _get_domain_df():
    '''
    Create a DataFrame with original and English labels for known domains.
    '''
    # Get the mapping dictionary and base dataframe from the respective functions
    print('\nPreparing JSON for Curlie labels...')
    mapping = _get_mapping_dict()
    df = _get_curlie_df()

    # Clean the URL column and aggregate duplicate entries into a list of unique curlielabels
    df['url'] = df['url'].apply(lambda url: url.replace('www.', ''))
    unique_domains = df.groupby('uid')['domain_label'].apply(lambda x: list(set(x)))
    df = df.drop('domain_label', axis=1) \
        .merge(unique_domains, on='uid', how='right') \
        .drop_duplicates('uid') \
        .rename(columns={'domain_label': 'curlielabels'})

    # Add the english labels (Remove any None elements from the list of labels and keep only unique labels)
    # In the final step, replace any empty lists with None
    df['curlielabels_en'] = df['curlielabels'] \
        .apply(lambda labels: [label if label.startswith('/en') else mapping.get(label, None) for label in labels]) \
        .apply(lambda labels_en: list(set([label for label in labels_en if label is not None]))) \
        .apply(lambda label_list: label_list if len(label_list) > 0 else None)

    df.rename(columns={'url': 'domain_url'}, inplace=True)
    return df[['domain_url', 'uid', 'curlielabels', 'curlielabels_en', 'lang']]
