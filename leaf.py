from pathlib import Path
import os

from pipeline import Pipeline
from server import PipelineServer
from node import BaseNode
from api import BaseClient


# Dynamically find mkcert's local CA
mkcert_ca = Path(os.popen("mkcert -CAROOT").read().strip()) / "rootCA.pem"
mkcert_ca = str(mkcert_ca)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ssl_certfile = os.path.join(BASE_DIR, "certs/certificate_leaf.pem")
ssl_keyfile = os.path.join(BASE_DIR, "certs/private_leaf.key")


client1 = BaseClient(name='client1', server_url="https://127.0.0.1:8001/node1/api/server")
connector1 = BaseNode(name='connector1', wait_for_connection=True)
node5 = BaseNode(name='node5', predecessors=[connector1])
node6 = BaseNode(name='node6', predecessors=[connector1])

pipeline = Pipeline([connector1, node5, node6])
service = PipelineServer(
    name="leaf", pipeline=pipeline, host="127.0.0.1", port=8001, 
    ssl_ca_certs=mkcert_ca, 
    ssl_certfile=ssl_certfile, 
    ssl_keyfile=ssl_keyfile,
    remote_clients=[client1]
)

try:
    service.run()
except KeyboardInterrupt:
    service.shutdown()