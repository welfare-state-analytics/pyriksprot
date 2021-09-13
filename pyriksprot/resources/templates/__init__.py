import glob
from os.path import dirname, join

from ...utility import strip_extensions, strip_paths

PARLA_TEMPLATES = strip_paths(glob.glob(join(dirname(__file__), "*.jinja")))
PARLA_TEMPLATES_SHORTNAMES = strip_extensions(PARLA_TEMPLATES)
