import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parents[2]))

from resilipipe import RESILIPIPE_DIR, RESOURCES_DIR
from resilipipe.conf.config import Configurator

if __name__ == '__main__':
    configurator = Configurator(modules_yaml=RESILIPIPE_DIR / 'conf/modules.yaml')
    modules = configurator.get_modules()

    # Perform the necessary preparation for each html module (e.g. downloading files)
    for module_tuple in modules:
        module_tuple[1].prepare(RESOURCES_DIR)
