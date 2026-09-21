import uuid


def generate_id(device_id: str | None = None) -> str:
    """Return an opaque, collision-resistant task identifier."""
    return uuid.uuid4().hex
