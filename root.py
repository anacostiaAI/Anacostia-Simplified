from pathlib import Path
import os

from node import BaseNode
from pipeline import Pipeline
from server import PipelineServer


# Dynamically find mkcert's local CA
mkcert_ca = Path(os.popen("mkcert -CAROOT").read().strip()) / "rootCA.pem"
mkcert_ca = str(mkcert_ca)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ssl_certfile = os.path.join(BASE_DIR, "certs/certificate_root.pem")
ssl_keyfile = os.path.join(BASE_DIR, "certs/private_root.key")

node1 = BaseNode(name='node1')
node2 = BaseNode(name='node2')
node3 = BaseNode(
    name='node3', 
    predecessors=[node1, node2], 
    remote_successors=["https://127.0.0.1:8001/connector1"], 
    client_url="https://127.0.0.1:8001/client1/api/client",
    wait_for_connection=True
)

pipeline = Pipeline([node1, node2, node3])
service = PipelineServer(
    name="root", pipeline=pipeline, host="127.0.0.1", port=8000, 
    ssl_ca_certs=mkcert_ca,
    ssl_certfile=ssl_certfile,
    ssl_keyfile=ssl_keyfile
)

try:
    service.run()
except KeyboardInterrupt:
    service.shutdown()