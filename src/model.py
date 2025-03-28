from dataclasses import dataclass
from datetime import datetime

@dataclass
class Product:
    id: str
    description: str
    price: float
    status: str
    created_at: datetime
    type: str