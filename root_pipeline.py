import signal
import threading

from typing import List, Iterable
from node import BaseNode, Connector
import networkx as nx
from fastapi import FastAPI
import uvicorn
import httpx
import asyncio



class RootPipeline:
    def __init__(self, nodes: Iterable[BaseNode]) -> None:

        self.node_dict = dict()
        self.graph = nx.DiGraph()

        # Add nodes into graph
        for node in nodes:
            self.graph.add_node(node)
            self.node_dict[node.name] = node

        # Add edges into graph
        for node in nodes:
            for predecessor in node.predecessors:
                self.graph.add_edge(predecessor, node)
        
        self.nodes: List[BaseNode] = list(nx.topological_sort(self.graph))
    
    def launch_nodes(self):
        print("Launching nodes")
        for node in self.nodes:
            node.start()
        print("All nodes launched")
    
    def terminate_nodes(self) -> None:
        print("Terminating nodes")
        for node in reversed(self.nodes):
            node.exit()
            node.join()
        print("All nodes terminated")
    
    def run(self) -> None:
        original_sigint_handler = signal.getsignal(signal.SIGINT)

        def _kill_webserver(sig, frame):
            print("\nCTRL+C Caught!; Shutting down nodes...")
            self.terminate_nodes()
            print("Nodes shutdown.")

            # register the original default kill handler once the pipeline is killed
            signal.signal(signal.SIGINT, original_sigint_handler)

        # register the kill handler for the webserver
        signal.signal(signal.SIGINT, _kill_webserver)

        print("Launching Pipeline...")
        self.launch_nodes()



class RootPipelineApp(FastAPI):
    def __init__(self, name: str, pipeline: RootPipeline, host: str = "127.0.0.1", port: int = 8000, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.pipeline = pipeline
        self.host = host
        self.port = port

        config = uvicorn.Config(self, host=self.host, port=self.port)
        self.server = uvicorn.Server(config)
        self.fastapi_thread = threading.Thread(target=self.server.run, name=name)

        for node in self.pipeline.nodes:
            subapp: Connector = node.setup_connector()
            subapp.set_host(self.host)
            subapp.set_port(self.port)
            self.mount(subapp.get_connector_prefix(), subapp)          # mount the BaseNodeApp to PipelineWebserver
        
        asyncio.run(self.connect())

    async def connect(self):
        async with httpx.AsyncClient() as client:
            # Connect each node to its remote successors
            task = []
            for node in self.pipeline.nodes:
                for connection in node.remote_successors:
                    json = {
                        "node_url": f"http://{self.host}:{self.port}/{node.name}",
                        "node_type": type(node).__name__
                    }
                    task.append(client.post(f"{connection}/connect", json=json))

            responses = await asyncio.gather(*task)

            # Extract the leaf URLs from the responses, connection is now established
            leaf_urls = []
            for response in responses:
                if response.status_code == 200:
                    response_json = response.json()
                    leaf_urls.append(response_json["node_url"])
            
            print(f"Root pipeline connected to these leaf URLs: {leaf_urls}")
            
    def run(self):
        original_sigint_handler = signal.getsignal(signal.SIGINT)

        def _kill_webserver(sig, frame):
            print(f"\nCTRL+C Caught!; Killing {self.name} Webservice...")
            self.server.should_exit = True
            self.fastapi_thread.join()
            print(f"Anacostia Webservice {self.name} Killed...")

            print("Killing pipeline...")
            self.pipeline.terminate_nodes()
            print("Pipeline Killed.")

            # register the original default kill handler once the pipeline is killed
            signal.signal(signal.SIGINT, original_sigint_handler)

        # register the kill handler for the webserver
        signal.signal(signal.SIGINT, _kill_webserver)
        self.fastapi_thread.start()

        # Launch the root pipeline
        self.pipeline.launch_nodes()

        # keep the main thread open; this is done to avoid an error in python 3.12 "RuntimeError: can't create new thread at interpreter shutdown"
        # and to avoid "RuntimeError: can't register atexit after shutdown" in python 3.9
        for thread in threading.enumerate():
            if thread.daemon or thread is threading.current_thread():
                continue
            thread.join()