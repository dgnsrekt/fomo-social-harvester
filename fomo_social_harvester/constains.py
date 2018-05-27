from pathlib import Path

BASEPATH = Path(__file__).parent
LOGPATH = BASEPATH / 'logs'
DATAPATH = BASEPATH / 'data'
BENCHPATH = BASEPATH / 'benchdata'

BAD_TWITTER_LINKS = BASEPATH / 'bad_twitter_links.log'

# DATAPATH.mkdir()
# datepath = DATAPATH / '2014-2-29'
# datepath.mkdir()
