from pipeline import Pipeline
from server import PipelineServer
from node import BaseNode


connector1 = BaseNode(name='connector1', wait_for_connection=True)
node5 = BaseNode(name='node5', predecessors=[connector1])
node6 = BaseNode(name='node6', predecessors=[connector1])

pipeline = Pipeline([connector1, node5, node6])
service = PipelineServer(name="leaf", pipeline=pipeline, host="127.0.0.1", port=8001)
service.run()
