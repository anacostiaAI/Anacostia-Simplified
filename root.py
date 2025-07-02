from node import BaseNode
from pipeline import Pipeline
from server import PipelineServer



node1 = BaseNode(name='node1')
node2 = BaseNode(name='node2')
node3 = BaseNode(
    name='node3', 
    predecessors=[node1, node2], 
    remote_successors=[{"node_url": "http://127.0.0.1:8001/connector1"}], 
    wait_for_connection=True
)

pipeline = Pipeline([node1, node2, node3])
service = PipelineServer(name="leaf", pipeline=pipeline, host="127.0.0.1", port=8000)
service.run()