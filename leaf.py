from leaf_pipeline import LeafPipeline, LeafPipelineApp
from node import BaseNode
from connector import LeafConnectorNode


connector1 = LeafConnectorNode(name='connector1')
node5 = BaseNode(name='node5', predecessors=[connector1])
node6 = BaseNode(name='node6', predecessors=[connector1])

pipeline = LeafPipeline([connector1, node5, node6])
service = LeafPipelineApp(name="leaf", pipeline=pipeline, host="127.0.0.1", port=8001)
service.run()
