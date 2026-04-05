from dataclasses import dataclass, field


@dataclass
class MediaItem:
    id: str
    path: str
    dir: str
    old_name: str
    ext: str
    metadata: dict = field(default_factory=lambda: {"id": "None"})
    new_name_only: str = ""
    full_target: str = ""

