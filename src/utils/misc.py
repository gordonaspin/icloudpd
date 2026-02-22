"""helpers"""
import logging
import hashlib
import os
import piexif
from piexif._exceptions import InvalidImageDataError

from utils.paths import local_download_path

logger = logging.getLogger(__name__)

def get_photo_metadata(photo, album, path, md5):
    """return dict of metadata"""
    d = {}
    d['photo'] = photo
    d['id'] = photo.id
    d['filename'] = photo.filename
    d['size'] = photo.size
    d['created'] = photo.created.isoformat()
    d['asset_date'] = photo.asset_date.isoformat()
    d['added_date'] = photo.added_date.isoformat()
    d['x'] = photo.dimensions[0]
    d['y'] = photo.dimensions[1]
    d['item_type'] = photo.item_type
    d['item_type_extension'] = os.path.splitext(photo.filename)
    d['path'] = path
    d['md5'] = md5
    d['album'] = album
    return d

def calculate_md5(path):
    """md5"""
    with open(path, 'rb') as f:
        data = f.read()
        return hashlib.md5(data).hexdigest()

def truncate_middle(string, length):
    """Truncates a string to a maximum length, inserting "..." in the middle"""
    if len(string) <= length:
        return string
    if length < 0:
        raise ValueError("n must be greater than or equal to 1")
    if length <= 3:
        return "..."[0:length]
    end_length = int(length) // 2 - 2
    start_length = length - end_length - 4
    end_length = max(end_length, 1)
    return f"{string[:start_length]}...{string[-end_length:]}"

def get_photo_exif(path):
    """Get EXIF date for a photo, return nothing if there is an error"""
    try:
        exif_dict = piexif.load(path)
        return exif_dict.get("Exif").get(36867)
    except (ValueError, InvalidImageDataError):
        logger.debug("Error fetching EXIF data for %s", path)
        return None

def set_photo_exif(path, date):
    """Set EXIF date on a photo, do nothing if there is an error"""
    try:
        exif_dict = piexif.load(path)
        exif_dict.get("1st")[306] = date
        exif_dict.get("Exif")[36867] = date
        exif_dict.get("Exif")[36868] = date
        exif_bytes = piexif.dump(exif_dict)
        piexif.insert(exif_bytes, path)
    except (ValueError, InvalidImageDataError):
        logger.debug("Error setting EXIF data for %s", path)

def autodelete_photos(icloud, folder_structure, directory):
    """
    Scans the "Recently Deleted" folder and deletes any matching files
    from the download directory.
    (I.e. If you delete a photo on your phone, it's also deleted on your computer.)
    """
    logger.info("Deleting any files found in 'Recently Deleted'...")

    recently_deleted = icloud.photos.albums["Recently Deleted"]

    for media in recently_deleted:
        created_date = media.created
        date_path = folder_structure.format(created_date)
        download_dir = os.path.join(directory, date_path)

        for size in [None, "original", "medium", "thumb"]:
            path = os.path.normpath(
                local_download_path(
                    media, size, download_dir))
            if os.path.exists(path):
                logger.info("Deleting %s!", path)
                os.remove(path)
