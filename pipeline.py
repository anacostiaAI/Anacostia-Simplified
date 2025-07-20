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
        print("Launching Pipeline...")
        self.launch_nodes()

