"""Main module to manage the download"""
import os
import sys
import logging
import socket
from datetime import datetime
from time import sleep, mktime
import itertools
import hashlib
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event

from tzlocal import get_localzone
import piexif
from piexif._exceptions import InvalidImageDataError
from pyicloud.services import PhotosService
from pyicloud.services.photos import SmartPhotoAlbum
from pyicloud.exceptions import (PyiCloud2SARequiredException,
                                 PyiCloudAPIResponseException,
                                 PyiCloudFailedLoginException)

import constants
from context import Context
from icloud.authentication import authenticate
from database import database
from utils.email_notifications import send_2sa_notification
from meta_data import PhotoMetaData, AlbumMetaData

logger = logging.getLogger("icloudpd")

class PhotoManager():
    """Main class to manage the download"""
    def __init__(self, ctx: Context):
        self.ctx = ctx
        self.api: PhotosService = self._connect()
        logger.info("fetching library information from iCloudService...")
        self.albums = self.api.photos.albums
        self.photos = self.api.photos.all
        self.event = Event()
        self.event.set()
        self.api.photos.exception_handler = self.photos_exception_handler

    def _connect(self) -> PhotosService:
        raise_authorization_exception = (
            self.ctx.smtp_username is not None
            or self.ctx.notification_email is not None
            or self.ctx.notification_script is not None
            or not sys.stdout.isatty()
        )
        try:
            logger.debug("connecting to iCloudService...")
            #icloud = PyiCloudService(
            api = authenticate(
                self.ctx.username,
                self.ctx.password,
                self.ctx.cookie_directory,
                raise_authorization_exception,
                client_id=os.environ.get("CLIENT_ID"),
                unverified_https=self.ctx.unverified_https)
        except PyiCloud2SARequiredException as ex:
            if self.ctx.notification_script is not None:
                logger.debug("executing notification script %s", self.ctx.notification_script)
                subprocess.call([self.ctx.notification_script])
            if self.ctx.smtp_username is not None or self.ctx.notification_email is not None:
                logger.debug("sending 2sa email notification")
                send_2sa_notification(
                    self.ctx.smtp_username,
                    self.ctx.smtp_password,
                    self.ctx.smtp_host,
                    self.ctx.smtp_port,
                    self.ctx.smtp_no_tls,
                    self.ctx.notification_email,
                )
            logger.error(ex)
            sys.exit(constants.ExitCode.EXIT_FAILED_2FA_REQUIRED.value)
        except PyiCloudFailedLoginException as ex:
            logger.error(ex)
            sys.exit(constants.ExitCode.EXIT_FAILED_LOGIN.value)

        return api

    def photos_count(self) -> int:
        """return how man photos/videos etc."""
        return len(self.photos)

    def albums_count(self) -> int:
        """return how man photos/videos etc."""
        return len(self.albums)

    def album_names(self) -> list[str]:
        """return all album names"""
        return [str(a) for a in self.albums]

    def smart_album_names(self) -> list[str]:
        """return all smart album names"""
        return [str(a) for a in self.albums if isinstance(a, SmartPhotoAlbum)]

    def photos_exception_handler(self, ex, retries):
        """Handles session errors in the PhotoAlbum photos iterator"""
        if "Invalid global session" in str(ex):
            if retries > constants.DOWNLOAD_MEDIA_MAX_RETRIES:
                logger.info("iCloud re-authentication failed! Please try again later.")
                raise ex
            logger.error("Session error, re-authenticating...")
            if retries > 1:
                # If the first reauthentication attempt failed,
                # start waiting a few seconds before retrying in case
                # there are some issues with the Apple servers
                sleep(constants.DOWNLOAD_RETRY_WAIT_SECONDS * retries)
            self.api = authenticate(self.ctx.username, self.ctx.password)
        else:
            if retries > constants.DOWNLOAD_MEDIA_MAX_RETRIES:
                logger.error("photos_exception_handler: giving up: %s", ex)
                raise ex
            logger.error("photos_exception_handler: retrying: %s", ex)
            sleep(constants.DOWNLOAD_RETRY_WAIT_SECONDS)

    def download_album(self, album: str):
        """downloads an album"""
        amd = AlbumMetaData(album)
        logger.info("processing album %s", album)
        photos = self.api.photos.albums.find(album)
        photos.exception_handler = self.photos_exception_handler
        photos_count = len(photos)

        # Optional: Only download the x most recent photos.
        if self.ctx.recent is not None:
            photos_count = self.ctx.recent
            photos = itertools.islice(photos, self.ctx.recent)

        plural_suffix = "" if photos_count == 1 else "s"
        video_suffix = ""
        if not self.ctx.skip_videos:
            video_suffix = " or video" if photos_count == 1 else " and videos"
        logger.info("%s: processing %s %s photo%s%s",
                    album, photos_count, self.ctx.size, plural_suffix, video_suffix)

        reached_date_since = False

        pending = set()
        with ThreadPoolExecutor() as tpe:
            for photo in iter(photos):
                created_date = self._created_date(album, photo)
                if self.ctx.date_since and (created_date < self.ctx.date_since):
                    reached_date_since = True
                    logger.info("%s: processed all assets more recent than %s",
                                    album, self.ctx.date_since)
                    break
                future = tpe.submit(self.download_photo, album, photo)
                pending = pending | set([future])
            while pending:
                done, pending = as_completed(pending), set()
                for future in done:
                    res = future.result()
                    if res is not None:
                        amd.assets.extend(future.result())

        if not reached_date_since:
            logger.info("%s: processed all assets", album)

        return amd

    # pylint: disable=too-many-branches, too-many-statements
    def download_photo(self, album, photo) -> list[PhotoMetaData] | None:
        """internal function for actually downloading the photos"""

        pdb = database.DatabaseHandler()
        pmd = None
        pmds = []

        if self.ctx.skip_videos and photo.item_type != "image":
            logger.info("skipping %s, only downloading photos.", photo.filename)
            return None
        if photo.item_type not in ["image", "movie"]:
            logger.info("skipping %s, only downloading"
                        " photos and videos. (Item type was: %s)",
                        photo.filename, photo.item_type)
            return None

        created_date = self._created_date(album, photo)
        download_dir = self._build_download_dir(album,
                                            created_date, datetime.fromtimestamp(0))

        download_size = self.ctx.size
        try:
            versions = photo.versions
        except KeyError as ex:
            logger.error("KeyError: %s attribute was not found in the photo fields!", ex)
            return None

        # if the size does not exist and is not original, then we can't force it
        if self.ctx.size not in versions and self.ctx.size != "original":
            if self.ctx.force_size:
                filename = photo.filename.encode("utf-8").decode("ascii", "ignore")
                logger.error("skipping %s, %s size does not exist. skipping...",
                                            filename, self.ctx.size)
                return None
            download_size = "original"

        download_path = self._local_download_path(photo, download_size, download_dir)
        short_path = self._short_path(download_path)

        file_exists = os.path.isfile(download_path)
        if file_exists:
            # for later: this crashes if download-size medium is specified
            file_size = os.stat(download_path).st_size
            version = photo.versions[download_size]

            photo_size = version["size"]
            if file_size != photo_size:
                download_path = f"-{photo_size}.".join(download_path.rsplit(".", 1))
                short_path = self._short_path(download_path)
                logger.info("deduplicated (size) %s file size %s photo size %s dated %s",
                            self._truncate_middle(short_path, 96),
                            file_size, photo_size, created_date)
                file_exists = os.path.isfile(download_path)
            if file_exists:
                logger.info("skipping (already exists) %s dated %s",
                            self._truncate_middle(short_path, 96),
                            created_date)
                if not pdb.asset_exists(short_path):
                    md5 = self._calculate_md5(download_path)
                    logger.info("updating %s md5 %s",
                                short_path, md5)
                else:
                    md5 = pdb.get_asset_md5(short_path)
                pmd = PhotoMetaData(album, short_path, md5, photo)
                pmd.filesize = os.stat(download_path).st_size
                pmds.append(pmd)
                # Check for multiple occurrences of same asset in
                # iCloud Photos library (happened with WhatsApp)

        if not file_exists:
            if self.ctx.only_print_filenames:
                print(download_path)
                pmd = PhotoMetaData(album, short_path, -1, photo)
            else:
                logger.info("downloading %s dated %s",
                            self._truncate_middle(short_path, 96),
                            created_date)
                download_result = self._download_media(photo, download_path, download_size)
                if download_result:
                    if (self.ctx.set_exif_datetime
                        and photo.filename.lower().endswith((".jpg", ".jpeg"))
                        and not self._get_photo_exif(download_path)):
                        # %Y:%m:%d looks wrong but it's the correct format
                        date_str = created_date.strftime("%Y-%m-%d %H:%M:%S%z")
                        logger.debug("setting EXIF timestamp for : %s %s",
                                        short_path, date_str)
                        self._set_photo_exif(download_path,
                                                        created_date.strftime("%Y:%m:%d %H:%M:%S"))
                    self._set_utime(download_path, created_date)
                    md5 = self._calculate_md5(download_path)
                    pmd = PhotoMetaData(album, short_path, md5, photo)
                    pmd.filesize = os.stat(download_path).st_size
                    pmds.append(pmd)

        # Also download the live photo if present
        if not self.ctx.skip_live_photos:
            pmd = self._download_live_photo(pdb, photo, album, download_dir)
            if pmd:
                pmds.append(pmd)

        return pmds

    def _created_date(self, album, photo) -> datetime:
        try:
            created_date = photo.created.astimezone(get_localzone())
        except (ValueError, OSError):
            logger.error("could not convert photo %s "
                                        "created date to local timezone (%s)",
                                        photo.filename, photo.created)
            created_date = photo.created
        return created_date

    def _download_live_photo(self, pdb, photo, album, download_dir) -> PhotoMetaData:
        size = self.ctx.live_photo_size + "_video"
        created_date = self._created_date(album, photo)

        pmd = None

        if size in photo.versions:
            version = photo.versions[size]
            filename = version["filename"]
            if self.ctx.live_photo_size != "original":
                # Add size to filename if not original
                filename = filename.replace(".MOV", f"-{self.ctx.self.ctx.live_photo_size}.MOV")
            download_path = os.path.join(download_dir, filename)
            short_path = self._short_path(download_path)
            file_exists = os.path.isfile(download_path)
            if self.ctx.only_print_filenames and not file_exists:
                print(download_path)
                pmd = PhotoMetaData(album, short_path, -1, photo)
            else:
                if file_exists:
                    file_size = os.stat(download_path).st_size
                    photo_size = version["size"]
                    if file_size != photo_size:
                        download_path = f"-{photo_size}.".join(
                            download_path.rsplit(".", 1))
                        logger.info("deduplicated live %s file size %s"
                                    " photo size %s dated %s",
                                    self._truncate_middle(
                                        short_path, 96),
                                    file_size, photo_size, created_date)
                        file_exists = os.path.isfile(download_path)
                    if file_exists:
                        logger.info("skipping live (already exists) %s dated %s",
                                    self._truncate_middle(
                                        short_path, 96),
                                    created_date)
                        if not pdb.asset_exists(short_path):
                            md5 = self._calculate_md5(download_path)
                            logger.info("%s: updating %s md5 %s",
                                        album, download_path, md5)
                        else:
                            md5 = pdb.get_asset_md5(short_path)

                        pmd = PhotoMetaData(album, short_path, md5, photo)
                        pmd.filesize = os.stat(download_path).st_size
                if not file_exists:
                    logger.info("downloading live %s dated %s",
                                self._truncate_middle(short_path, 96),
                                created_date)
                    self._download_media(photo, download_path, size)
                    md5 = self._calculate_md5(download_path)

                    pmd = PhotoMetaData(album, short_path, md5, photo)
        return pmd

    def autodelete_photos(self):
        """
        Scans the "Recently Deleted" folder and deletes any matching files
        from the download directory.
        (I.e. If you delete a photo on your phone, it's also deleted on your computer.)
        """
        logger.info("Deleting any files found in 'Recently Deleted'...")

        recently_deleted = self.api.photos.albums["Recently Deleted"]

        for media in recently_deleted:
            created_date = media.created
            date_path = self.ctx.folder_structure.format(created_date)
            download_dir = os.path.join(self.ctx.directory, date_path)

            for size in [None, "original", "medium", "thumb"]:
                path = os.path.normpath(
                    self._local_download_path(
                        media, size, download_dir))
                if os.path.exists(path):
                    logger.info("deleting %s!", path)
                    os.remove(path)

    def _build_download_dir(self, album, created_date, default_date):
        """return download folder path"""
        try:
            if self.ctx.folder_structure.lower() == "none":
                folder_path = ""
            elif self.ctx.folder_structure.lower() == "album":
                folder_path = album
            else:
                folder_path = self.ctx.folder_structure.format(created_date)
        except ValueError:  # pragma: no cover
            folder_path = self.ctx.folder_structure.format(default_date)

        return os.path.normpath(os.path.join(self.ctx.directory, folder_path))

    def _local_download_path(self, media, size, download_dir):
        """Returns the full download path, including size"""
        filename = self._filename_with_size(media, size)
        download_path = os.path.join(download_dir, filename)
        return download_path

    def _filename_with_size(self, media, size):
        """Returns the filename with size, e.g. IMG1234.jpg, IMG1234-small.jpg"""
        # Strip any non-ascii characters.
        filename = media.filename.encode("utf-8").decode("ascii", "ignore")
        if size == 'original':
            return filename
        return f"-{size}.".join(filename.rsplit(".", 1))

    def _short_path(self, download_path):
        return download_path[len(self.ctx.directory)+1:]

    def _truncate_middle(self, string, length):
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

    def _calculate_md5(self, path):
        """md5"""
        with open(path, 'rb') as f:
            data = f.read()
            return hashlib.md5(data).hexdigest()

    def _get_photo_exif(self, path):
        """Get EXIF date for a photo, return nothing if there is an error"""
        try:
            exif_dict = piexif.load(path)
            return exif_dict.get("Exif").get(36867)
        except (ValueError, InvalidImageDataError):
            logger.debug("Error fetching EXIF data for %s", path)
            return None

    def _set_photo_exif(self, path, date):
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

    def _set_utime(self, download_path, created_date):
        """Set date & time of the file"""
        ctime = mktime(created_date.timetuple())
        os.utime(download_path, (ctime, ctime))

    def _download_media(self, photo, download_path, size):
        """Download the photo to path, with retries and error handling"""

        # get back the directory for the file to be downloaded and create it if not there already
        download_dir = os.path.dirname(download_path)

        if not os.path.exists(download_dir):
            try:
                os.makedirs(download_dir)
            except OSError:  # pragma: no cover
                pass         # pragma: no cover

        for retries in range(constants.DOWNLOAD_MEDIA_MAX_RETRIES):
            try:
                self.event.wait()
                photo_data = photo.download(size)
                if photo_data:
                    temp_download_path = download_path + ".part"
                    with open(temp_download_path, "wb") as file_obj:
                        file_obj.write(photo_data)

                    os.rename(temp_download_path, download_path)
                    self._update_mtime(photo, download_path)
                    return True

                logger.error("Could not find URL to download %s for size %s!",
                    photo.filename, size)
                break

            except (ConnectionError, socket.timeout, PyiCloudAPIResponseException) as ex:
                if "Invalid global session" in str(ex):
                    logger.error("Session error, re-authenticating...")
                    self.event.clear()
                    if retries > 0:
                        # If the first reauthentication attempt failed,
                        # start waiting a few seconds before retrying in case
                        # there are some issues with the Apple servers
                        sleep(constants.DOWNLOAD_RETRY_WAIT_SECONDS)

                    self._connect()
                else:
                    # you end up here when p.e. throttleing by Apple happens
                    wait_time = (retries + 1) * constants.DOWNLOAD_RETRY_WAIT_SECONDS
                    logger.error("Error %s downloading %s to %s, retrying after %d seconds...",
                                ex, photo.filename, download_path, wait_time)
                    self.event.clear()
                    sleep(wait_time)

            except IOError:
                logger.error(
                    "IOError while writing file to %s! "
                    "You might have run out of disk space, or the file "
                    "might be too large for your OS. "
                    "Skipping this file...", download_path
                )
                break
            finally:
                self.event.set()

        else:
            logger.warning("Could not download %s! Please try again later.", photo.filename)

        return False

    def _update_mtime(self, photo, download_path):
        """Set the modification time of the downloaded file to the photo creation date"""
        if photo.created:
            created_date = None
            try:
                created_date = photo.created.astimezone(
                    get_localzone())
            except (ValueError, OSError):
                # We already show the timezone conversion error in base.py,
                # when generating the download directory.
                # So just return silently without touching the mtime.
                return
            self._set_utime(download_path, created_date)
