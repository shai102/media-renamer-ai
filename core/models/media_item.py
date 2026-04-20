from dataclasses import dataclass, field


@dataclass
class MediaItem:
    id: str
    path: str
    dir: str
    old_name: str
    ext: str
    source_path: str = ""
    organize_root: str = ""
    metadata: dict = field(default_factory=lambda: {"id": "None"})
    new_name_only: str = ""
    full_target: str = ""
    display_title: str = ""
    display_match_id: str = ""
    display_target: str = ""
    status_text: str = "待命"
    parse_source: str = ""
