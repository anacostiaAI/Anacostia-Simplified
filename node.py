from typing import List, Dict
from threading import Event, Thread
import random
import asyncio
import httpx

from fastapi import FastAPI, status
from utils import ConnectionModel



class Connector(FastAPI):
    def __init__(self, node: 'BaseNode', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.node = node
        self.host = None
        self.port = None

        @self.post("/connect", status_code=status.HTTP_200_OK)
        async def connect(root: ConnectionModel) -> ConnectionModel:
            self.node.add_remote_predecessor(root.node_url)
            return ConnectionModel(node_url=f"http://{self.host}:{self.port}/{self.node.name}", node_type=type(self.node).__name__)
        
        @self.post("/forward_signal", status_code=status.HTTP_200_OK)
        async def forward_signal(root: ConnectionModel):
            self.node.predecessors_events[root.node_url].set()
            return {"message": "Signalled predecessors"}

        @self.post("/backward_signal", status_code=status.HTTP_200_OK)
        async def backward_signal(leaf: ConnectionModel):
            self.node.successor_events[leaf.node_url].set()
            return {"message": "Signalled predecessors"}
    
    def set_host(self, host: str):
        self.host = host
    
    def set_port(self, port: int):
        self.port = port

    def get_node_prefix(self):
        return f"/{self.node.name}"
    
    def get_endpoint(self):
        return f"{self.get_node_prefix()}/home"
    
    def get_status_endpoint(self):
        return f"{self.get_node_prefix()}/status"
    


class BaseNode(Thread):
    def __init__(self, name: str, predecessors: List['BaseNode'] = None, remote_successors: List[str] = None):
        self.predecessors = list() if predecessors is None else predecessors
        self.remote_predecessors = list()
        self.predecessors_events: Dict[str, Event] = {predecessor.name: Event() for predecessor in self.predecessors}

        self.successors: List[BaseNode] = list()
        self.remote_successors = list() if remote_successors is None else remote_successors
        self.successor_events: Dict[str, Event] = {connection: Event() for connection in self.remote_successors}

        # add node to each predecessor's successors list and create an event for each predecessor's successor_events
        for predecessor in self.predecessors:
            predecessor.successors.append(self)
            predecessor.successor_events[name] = Event()

        self.exit_event = Event()
        self.pause_event = Event()
        self.pause_event.set()

        super().__init__(name=name)

    def add_remote_predecessor(self, url: str):
        self.remote_predecessors.append(url)
        self.predecessors_events[url] = Event()
    
    def initialize_app_connector(self):
        self.app = Connector(self)
        return self.app
    
    async def signal_successors(self):
        for successor in self.successors:
            successor.predecessors_events[self.name].set()
        
        try:
            async with httpx.AsyncClient() as client:
                tasks = []
                for successor_url in self.remote_successors:
                    json = {
                        "node_url": f"http://{self.app.host}:{self.app.port}/{self.name}",
                        "node_type": type(self).__name__
                    }
                    tasks.append(client.post(f"{successor_url}/forward_signal", json=json))
                
                await asyncio.gather(*tasks)
        except httpx.ConnectError:
            print(f"Failed to signal successors from {self.name}")
            self.exit()

    def wait_for_successors(self):
        for event in self.successor_events.values():
            event.wait()
        
        for event in self.successor_events.values():
            event.clear()
    
    async def signal_predecessors(self):
        for predecessor in self.predecessors:
            predecessor.successor_events[self.name].set()

        try:
            async with httpx.AsyncClient() as client:
                tasks = []
                for predecessor_url in self.remote_predecessors:
                    json = {
                        "node_url": f"http://{self.app.host}:{self.app.port}/{self.name}",
                        "node_type": type(self).__name__
                    }
                    tasks.append(client.post(f"{predecessor_url}/backward_signal", json=json))

                await asyncio.gather(*tasks)
        except httpx.ConnectError:
            print(f"Failed to signal predecessors from {self.name}")
            self.exit()
            
    def wait_for_predecessors(self):
        for event in self.predecessors_events.values():
            event.wait()
        
        for event in self.predecessors_events.values():
            event.clear()
    
    def exit(self):
        # set all events so loop can continue to next checkpoint and break out of loop
        self.pause_event.set()
        self.exit_event.set()

        for event in self.successor_events.values():
            event.set()
        
        for event in self.predecessors_events.values():
            event.set()
    
    async def run_async(self):
        while self.exit_event.is_set() is False:

            if self.exit_event.is_set(): return
            print(f'{self.name} waiting for predecessors')
            self.wait_for_predecessors()

            if self.exit_event.is_set(): return
            print(f'{self.name} signalling successors')
            await self.signal_successors()

            print(f'{self.name} is running')
            await asyncio.sleep(random.randint(1, 3))
            print(f'{self.name} is done')

            if self.exit_event.is_set(): return
            print(f'{self.name} waiting for successors')
            self.wait_for_successors()

            if self.exit_event.is_set(): return
            print(f'{self.name} signalling predecessors')
            await self.signal_predecessors()
    
    def run(self) -> None:
        asyncio.run(self.run_async())