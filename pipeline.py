from typing import List, Iterable, Union
from node import BaseNode
import networkx as nx
import signal



class Pipeline:
    def __init__(self, nodes: Iterable[ Union[BaseNode] ]) -> None:

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

