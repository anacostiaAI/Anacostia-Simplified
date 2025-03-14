from pydantic import BaseModel


class ConnectionModel(BaseModel):
    node_url: str
    node_type: str