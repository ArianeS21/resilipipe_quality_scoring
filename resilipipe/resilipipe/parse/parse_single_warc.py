import argparse
import os
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

from resilipipe import RESOURCES_DIR, RESILIPIPE_DIR
from resilipipe.conf.config import Configurator
from resilipipe.parse.warc_preprocessing import parse_warc


def parse_single_warc(input_file: Path, output_file: Path):
    """Parses a single WARC file and stores the result to a Parquet file."""
    configurator = Configurator(modules_yaml=RESILIPIPE_DIR / 'conf/modules.yaml')
    modules = configurator.get_modules()
    pa_schema = pa.schema(configurator.get_pyarrow_schema())
    print("Loading modules...")
    module_input = {module_tuple[0]: module_tuple[1].load_input(RESOURCES_DIR) for module_tuple in modules}
    schema_metadata = configurator.get_schema_metadata()

    results = parse_warc(warc_input=str(input_file), modules=modules, module_input=module_input,
                         schema_metadata=schema_metadata)
    table = pa.Table.from_pylist(list(results), schema=pa_schema)
    pq.write_table(table, output_file)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('input', type=Path)
    parser.add_argument('output', type=Path)

    args = parser.parse_args()

    parse_single_warc(args.input, args.output)


if __name__ == '__main__':
    main()
