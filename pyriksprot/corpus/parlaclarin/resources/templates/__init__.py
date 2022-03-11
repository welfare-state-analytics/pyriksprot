from glob import glob
from os.path import basename, dirname, join, splitext

PARLA_TEMPLATES = [basename(filename) for filename in glob(join(dirname(__file__), "*.jinja"))]
PARLA_TEMPLATES_SHORTNAMES = [splitext(x)[0] for x in PARLA_TEMPLATES]
