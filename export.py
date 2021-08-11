import sys

from unscne.graph import connect_graph
from unscne.util import LogLevel, log

if len(sys.argv) != 2:
    log(f"Usage: python export.py <PATH_TO_EXPORT>", LogLevel.ERROR)
else:
    target_path = sys.argv[1]
    g = connect_graph()
    log(f"Writing json lines to {target_path} (this might take a while)..")
    query = f"CALL apoc.export.json.all('{target_path}',{{useTypes:true}})"
    g.execute_query_without_transaction(query)
    log("Done.")
