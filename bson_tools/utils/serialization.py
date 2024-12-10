import json
import yaml
from datetime import datetime
from bson import ObjectId
from typing import Any, Union

class BSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for BSON-specific types"""
    def default(self, obj: Any) -> Any:
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def serialize_output(data: Any, format: str = 'json') -> str:
    """Serialize data to the specified format"""
    if format == 'json':
        return json.dumps(data, cls=BSONEncoder, indent=2)
    elif format == 'yaml':
        return yaml.dump(data, default_flow_style=False)
    else:
        raise ValueError(f"Unsupported format: {format}")
