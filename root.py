from node import BaseNode
from root_pipeline import RootPipeline, RootPipelineApp



node1 = BaseNode(name='node1')
node2 = BaseNode(name='node2')
node3 = BaseNode(name='node3', predecessors=[node1, node2], remote_successors=["http://127.0.0.1:8001/connector1"])

pipeline = RootPipeline([node1, node2, node3])
service = RootPipelineApp(name="leaf", pipeline=pipeline, host="127.0.0.1", port=8000)
service.run()