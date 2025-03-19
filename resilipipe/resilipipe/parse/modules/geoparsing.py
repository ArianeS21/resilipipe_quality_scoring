from pathlib import Path
import pyarrow as pa
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from typing import Dict, Sequence, Union

from resiliparse.parse.html import HTMLTree
from resilipipe.parse.modules.abstract import HTMLModule


class Module(HTMLModule):
    @staticmethod
    def prepare(resources_path: Path):
        pass

    @staticmethod
    def load_input(resources_path: Path):
        return {}

    @staticmethod
    def get_pyarrow_schema() -> Sequence[pa.field]:
        schema = [
            pa.field('address',
                     pa.list_(pa.struct([
                         ('loc', pa.struct([
                             ('streetAddress', pa.string(), True),
                             ('postalCode', pa.string(), True),
                             ('addressLocality', pa.string(), True),
                             ('addressCountry', pa.string(), True)
                         ])),
                         ('geo', pa.struct([
                             ('latitude', pa.float64(), True),
                             ('longitude', pa.float64(), True)
                         ]))
                     ])), True)
        ]
        return schema

    @staticmethod
    def get_spark_schema() -> Sequence[StructField]:
        schema = [
            StructField('address', ArrayType(StructType([
                StructField('loc', StructType([
                    StructField('streetAddress', StringType(), True),
                    StructField('postalCode', StringType(), True),
                    StructField('addressLocality', StringType(), True),
                    StructField('addressCountry', StringType(), True),
                ]), True),
                StructField('geo', StructType([
                    StructField('latitude', StringType(), True),
                    StructField('longitude', StringType(), True)
                ]), True)
            ])), True)
        ]
        return schema

    @staticmethod
    def process_record(tree: HTMLTree, plain_text: str, language: str, json_ld: Sequence, microdata: Sequence,
                       **kwargs) -> Dict[str, Union[str, None]]:
        """
        Process Microformat and/or plain_text to extract locations.

        Args:
            tree (HTMLTree): HTML tree obtained from the parsed HTML content.
            plain_text (str): Plain text content.
            language (str): Language information.
            json_ld (Sequence): JSON Linked Data (JSON-LD) sequence.
            microdata (Sequence): Microdata sequence.
            **kwargs: Additional keyword arguments, e.g., stopwords, location_dict.

        Returns:
            dictionary: A Dictionary with the following key:
                - 'address': List of dictionaries containing extracted location and coordinates.
        """
        address = []
        if json_ld:
            address = extract_location_and_coordinates_json_ld(json_ld)
        if microdata:
            address_microdata = extract_location_and_coordinates_microdata(microdata)
            if address and address_microdata:
                [address.append(i) for i in address_microdata if i not in address]
            elif address_microdata:
                address = address_microdata

        address = None if len(address) == 0 else address
        return {"address": address}


def traverse_microdata(item):
    """
    Traverses microdata structured JSON and extracts location and coordinates.

    Args:
        item (dict): A JSON object representing microdata.

    Returns:
        dict: A dictionary containing location and coordinates extracted from microdata.
    """
    result = None
    if isinstance(item, dict):
        location = None
        coordinates = None
        for key, value in item.items():
            try:
                if key == "properties" and value:
                    if value.get("address", {}).get("properties", {}):
                        loc = value.get("address", {}).get("properties", {})
                        if loc:
                            location = {"streetAddress": str(loc.get("streetAddress", None)),
                                        "postalCode": str(loc.get("postalCode", None)),
                                        "addressLocality": str(loc.get("addressLocality", None)),
                                        "addressCountry": str(loc.get("addressCountry", None))}
                    if value.get("geo", {}).get("properties", {}):
                        coordinates = value.get("geo", {}).get("properties", {})
                        if coordinates:
                            if not coordinates.get("longitude", {}) and len(coordinates["latitude"]) == 2:
                                coordinates = {"latitude": coordinates["latitude"][0],
                                               "longitude": coordinates["latitude"][1]}
                            coordinates = {"latitude": float(coordinates["latitude"]),
                                           "longitude": float(coordinates["longitude"])}
                    else:
                        for k, v in value.items():
                            if isinstance(v, dict):
                                result = traverse_microdata(v)
                                if result:
                                    location = result["loc"]
                                    coordinates = result["geo"]
            except:
                return None
        if location or coordinates:
            return {"loc": location, "geo": coordinates}
        return None

    elif isinstance(item, list):
        for subitem in item:
            result = traverse_microdata(subitem)
    return result


def extract_location_and_coordinates_microdata(data: Sequence):
    """
    Extracts location and coordinates from microdata.

    Args:
        data (list): A list of microdata objects.

    Returns:
        list: A list of dictionaries containing extracted location and coordinates.
    """
    locations = []
    for item in data:
        result = traverse_microdata(item)
        if result:
            locations.append(result)
    return locations


def traverse_jsonld(item):
    """
    Traverses JSON-LD structured JSON and extracts location and coordinates.

    Args:
        item (dict): A JSON object representing JSON-LD.

    Returns:
        dict: A dictionary containing location and coordinates extracted from JSON-LD.
    """
    result = None
    if isinstance(item, dict):
        location = None
        coordinates = None
        for key, value in item.items():
            try:
                if key == "address" and value:
                    location = {"streetAddress": str(value.get("streetAddress", None)),
                                "postalCode": str(value.get("postalCode", None)),
                                "addressLocality": str(value.get("addressLocality", None)),
                                "addressCountry": str(value.get("addressCountry", None))}

                if key == "geo" and value:
                    if isinstance(value, dict):
                        coordinates = {"latitude": float(value["latitude"]), "longitude": float(value["longitude"])}
                    else:
                        coordinates = {"latitude": float(value.split(",")[0]), "longitude": float(value.split(",")[1])}

                else:
                    if isinstance(value, dict):
                        result = traverse_jsonld(value)
                        if result:
                            location = result["loc"]
                            coordinates = result["geo"]

            except:
                return None
        if location or coordinates:
            return ({"loc": location, "geo": coordinates})
        return None

    elif isinstance(item, list):
        for subitem in item:
            result = traverse_jsonld(subitem)
    return result


def extract_location_and_coordinates_json_ld(data: Sequence):
    """
    Extracts location and coordinates from JSON-LD.

    Args:
        data (list): A list of JSON-LD objects.

    Returns:
        list: A list of dictionaries containing extracted location and coordinates.
    """
    locations = []
    for item in data:
        result = traverse_jsonld(item)
        if result:
            locations.append(result)
    return locations
