from typing import NamedTuple, Dict
from datetime import datetime


class FinishedTask(NamedTuple):
    """
    Finished task structure
    """
    result_path: str
    kb_size: float
    timestamp: datetime

    def to_dict(self):
        data = self._asdict()
        data['timestamp'] = data['timestamp'].isoformat()
        return data

    @classmethod
    def from_dict(cls, data: Dict):
        data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        return cls(**data)
