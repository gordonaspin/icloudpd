"""Context class"""
from dataclasses import dataclass
from datetime import datetime

@dataclass
class Context():
    """context object"""
    directory: str
    username: str
    password: str | None
    cookie_directory: str
    size: str
    live_photo_size: str
    recent: int
    date_since: datetime
    newest: bool
    album: str
    all_albums: bool
    skip_smart_folders: bool
    skip_all_photos: bool
    list_albums: bool
    sort: str
    skip_videos: bool
    skip_live_photos: bool
    force_size: bool
    auto_delete: bool
    only_print_filenames: bool
    folder_structure: str
    list_duplicates: bool
    create_json_listing: bool
    set_exif_datetime: bool
    smtp_username: str
    smtp_password: str
    smtp_host: str
    smtp_port: int
    smtp_no_tls: bool
    notification_email: str
    notification_script: str
    logging_config: str
    unverified_https: bool
