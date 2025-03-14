import asyncio
from threading import Event
import httpx

from fastapi import status

from utils import ConnectionModel
from node import BaseApp
from node import BaseNode



class BaseConnectorApp(BaseApp):
    def __init__(self, node: 'BaseConnectorNode', *args, **kwargs):
        super().__init__(node=node, *args, **kwargs)

        @self.post("/connect", status_code=status.HTTP_200_OK)
        async def connect(root: ConnectionModel) -> ConnectionModel:
            self.node.add_remote_predecessor(root.node_url)
            return ConnectionModel(node_url=f"http://{self.host}:{self.port}/{self.node.name}", node_type=type(self.node).__name__)
        
        @self.post("/forward_signal", status_code=status.HTTP_200_OK)
        async def forward_signal(root: ConnectionModel):
            self.node.predecessors_events[root.node_url].set()
            return {"message": "Signalled predecessors"}



class BaseConnectorNode(BaseNode):
    def __init__(self, name: str):
        super().__init__(name=name)
        self.remote_predecessors = list()
    
    def add_remote_predecessor(self, url: str):
        self.predecessors_events[url] = Event()
    
    def get_app(self):
        self.app = BaseConnectorApp(self)
        return self.app
    
    async def signal_predecessors(self):
        super().signal_predecessors()

        try:
            async with httpx.AsyncClient() as client:
                tasks = []
                for predecessor_url in self.predecessors_events.keys():
                    json = {
                        "node_url": f"http://{self.app.host}:{self.app.port}/{self.name}",
                        "node_type": type(self).__name__
                    }
                    tasks.append(client.post(f"{predecessor_url}/backward_signal", json=json))

                await asyncio.gather(*tasks)
        except httpx.ConnectError:
            print(f"Failed to signal predecessors from {self.name}")
            self.exit()
            
    async def run_async(self):
        print(f'{self.name} waiting for root predecessors to connect')

        while len(self.predecessors_events) <= 0:
            await asyncio.sleep(0.1)
            if self.exit_event.is_set(): return

        print(f'{self.name} connected to root predecessors {list(self.predecessors_events.keys())}')

        while self.exit_event.is_set() is False:

            if self.exit_event.is_set(): return
            print(f'{self.name} waiting for predecessors')
            self.wait_for_predecessors()

            if self.exit_event.is_set(): return
            print(f'{self.name} signalling successors')
            await self.signal_successors()

            if self.exit_event.is_set(): return
            print(f'{self.name} waiting for successors')
            self.wait_for_successors()

            if self.exit_event.is_set(): return
            print(f'{self.name} signalling predecessors')
            await self.signal_predecessors()
    