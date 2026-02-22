"""Main script that uses Click to parse command-line arguments"""
from __future__ import print_function

import datetime
import itertools
import json
import logging
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import click
import urllib3
from tzlocal import get_localzone
from urllib3.exceptions import InsecureRequestWarning
from pyicloud.exceptions import (PyiCloud2SARequiredException,
                                 PyiCloudAPIResponseException,
                                 PyiCloudFailedLoginException)
from pyicloud.services.photos import SmartPhotoAlbum

import constants
from utils.email_notifications import send_2sa_notification
from utils.paths import build_download_dir, local_download_path
from utils.thread_safe import ThreadSafeDict, thread_safe_dict_to_dict_recursive
from utils.misc import (
    autodelete_photos,
    calculate_md5,
    get_photo_metadata,
    get_photo_exif,
    set_photo_exif,
    truncate_middle,
)
from icloud.authentication import authenticate
from logger.logger import setup_logger
from database import database
import download

logger = logging.getLogger("icloudpd")

urllib3.disable_warnings(category=InsecureRequestWarning)

@click.command(
        context_settings={"help_option_names": ["-h", "--help"]},
        options_metavar="<options>")
@click.option("-d", "--directory",
              help="Local directory that should be used for download",
              type=click.Path(exists=True),
              metavar="<directory>")
@click.option("-u", "--username",
              help="Your iCloud username or email address",
              metavar="<username>")
@click.option("-p", "--password",
              help="Your iCloud password (default: use PyiCloud keyring or prompt for password)",
              metavar="<password>")
@click.option("--cookie-directory",
              help="Directory to store cookies for authentication (default: ~/.pyicloud)",
              metavar="</cookie/directory>",
              default="~/.pyicloud")
@click.option("--size",
              help="Image size to download (default: original)",
              type=click.Choice(["original", "medium", "thumb"]),
              default="original")
@click.option("--live-photo-size",
              help="Live Photo video size to download (default: original)",
              type=click.Choice(["original", "medium", "thumb"]),
              default="original")
@click.option("--recent",
              help="Number of recent photos to download (default: download all photos)",
              type=click.IntRange(0))
@click.option('--date-since',
              help="Download only assets newer than date-since",
              type=click.DateTime(formats=["%Y-%m-%d", "%Y-%m-%d-%H:%M:%S"]))
@click.option('--newest',
              help="Download assets newer than newest known. Overrides --date-since value.",
              is_flag=True)
@click.option("--until-found",
              help="Download most recently added photos until we find x number of previously "
                   "downloaded consecutive photos (default: download all photos)",
              type=click.IntRange(0))
@click.option("-a", "--album",
              help="Album to download (default: All Photos)",
              metavar="<album>",
              default="All Photos")
@click.option("--all-albums",
              help="Download all albums",
              is_flag=True)
@click.option("--skip-smart-folders",
              help="Exclude smart folders from listing or download: All Photos, Time-lapse, "\
                   "Videos, Slo-mo, Bursts, Favorites, Panoramas, Screenshots, Live, "\
                    "Recently Deleted, Hidden",
              is_flag=True)
@click.option("--skip-All-Photos",
              help="Exclude the smart folders 'All Photos' from listing or download",
              is_flag=True)
@click.option("-l", "--list-albums",
              help="Lists the avaliable albums and exits",
              is_flag=True)
@click.option("-s", "--sort",
              help="Sort album names (default: desc)",
              type=click.Choice(["asc", "desc"]),
              default="desc")
@click.option("--skip-videos",
              help="Don't download any videos (default: Download all photos and videos)",
              is_flag=True)
@click.option("--skip-live-photos",
              help="Don't download any live photos (default: Download live photos)",
              is_flag=True,)
@click.option("--force-size",
              help="Only download the requested size (default: download original "\
                   "if size is not available)",
              is_flag=True,)
@click.option("--auto-delete",
              help="Scans the Recently Deleted folder and deletes any files found in there. "\
                   "(If you restore the photo in iCloud, it will be downloaded again.)",
              is_flag=True)
@click.option("--only-print-filenames",
              help="Only prints filenames that will be downloaded "\
                   "(Does not download or delete any files.)",
              is_flag=True)
@click.option("--folder-structure",
              help="Folder structure (default: {:%Y/%m/%d}). If set to 'none' all photos will "\
                   "just be placed into the download directory, if set to 'album' photos will "\
                   "be placed in a folder named as the album into the download directory",
              metavar="<folder_structure>",
              default="{:%Y/%m/%d}",)
@click.option("--list-duplicates",
              help="List files that are duplicates by the file content md5 hash",
              is_flag=True)
@click.option("--create-json-listing",
              help="Creates a catalog.json file listing of the albums/assets processed in the "\
                   "folder specified by directory option",
                   is_flag=True)
@click.option("--set-exif-datetime",
              help="Set DateTimeOriginal exif tag from file creation date, if it doesn't exist.",
              is_flag=True)
@click.option("--smtp-username",
              help="SMTP username for sending email when two-step authentication expires.",
              metavar="<smtp_username>")
@click.option("--smtp-password",
              help="SMTP password for sending email when two-step authentication expires.",
              metavar="<smtp_password>")
@click.option("--smtp-host",
              help="SMTP server host. Defaults to: smtp.gmail.com",
              metavar="<smtp_host>",
              default="smtp.gmail.com")
@click.option("--smtp-port",
              help="SMTP server port. Default: 587 (Gmail)",
              metavar="<smtp_port>",
              type=click.IntRange(0),
              default=587)
@click.option("--smtp-no-tls",
              help="Pass this flag to disable TLS for SMTP (TLS is required for Gmail)",
              metavar="<smtp_no_tls>",
              is_flag=True)
@click.option("--notification-email",
              help="Email address to receive email notifications. Default: SMTP username",
              metavar="<notification_email>")
@click.option("--notification-script",
              help="Runs an external script when two factor authentication expires. "\
              "(path required: /path/to/my/script.sh)",
              type=click.Path(), )
@click.option("--log-level",
              help="Log level (default: info)",
              type=click.Choice(["debug", "info", "error"]),
              default="info")
@click.option("--no-progress-bar",
              help="Disables the one-line progress bar and prints log messages on separate lines)",
              is_flag=True)
@click.option("--unverified-https",
              help="Overrides default https context with unverified https context",
              is_flag=True)

@click.version_option()
# pylint: disable-msg=too-many-arguments,too-many-statements
# pylint: disable-msg=too-many-branches,too-many-locals

def main(
        directory,
        username,
        password,
        cookie_directory,
        size,
        live_photo_size,
        recent,
        date_since,
        newest,
        until_found,
        album,
        all_albums,
        skip_smart_folders,
        skip_all_photos,
        list_albums,
        sort,
        skip_videos,
        skip_live_photos,
        force_size,
        auto_delete,
        only_print_filenames,
        folder_structure,
        list_duplicates,
        create_json_listing,
        set_exif_datetime,
        smtp_username,
        smtp_password,
        smtp_host,
        smtp_port,
        smtp_no_tls,
        notification_email,
        log_level,
        no_progress_bar,
        notification_script,
        unverified_https,
):
    """Download all iCloud photos to a local directory"""
    start = datetime.datetime.now()
    setup_logger()

    mdb = None
    if directory:
        database.setup_database(directory)
        #setup_database_logger()
        mdb = database.DatabaseHandler()

    if only_print_filenames or list_albums:
        if not log_level == "debug":
            logger.disabled = True
    else:
        # Need to make sure disabled is reset to the correct value,
        # because the logger instance is shared between tests.
        logger.disabled = False
        if log_level == "debug":
            logger.setLevel(logging.DEBUG)
        elif log_level == "info":
            logger.setLevel(logging.INFO)
        elif log_level == "error":
            logger.setLevel(logging.ERROR)

    logger.debug("directory: %s", directory)
    logger.debug("username: %s", username)
    logger.debug("cookie_directory: %s", cookie_directory)
    logger.debug("size: %s", size)
    logger.debug("live_photo_size %s", live_photo_size)
    logger.debug("recent: %s", recent)
    logger.debug("date_since: %s", date_since)
    logger.debug("newest: %s", newest)
    logger.debug("until_found: %s", until_found)
    logger.debug("album: %s", album)
    logger.debug("all_albums: %s", all_albums)
    logger.debug("skip_smart_folders: %s", skip_smart_folders)
    logger.debug("skip_all_photos: %s", skip_all_photos)
    logger.debug("list_albums: %s", list_albums)
    logger.debug("sort: %s", sort)
    logger.debug("skip_videos: %s", skip_videos)
    logger.debug("skip_live_photos: %s", skip_live_photos)
    logger.debug("force_size: %s", force_size)
    logger.debug("auto_delete: %s", auto_delete)
    logger.debug("only_print_filenames: %s", only_print_filenames)
    logger.debug("folder_structure: %s", folder_structure)
    logger.debug("list_duplicates: %s", list_duplicates)
    logger.debug("set_exif_datetime: %s", set_exif_datetime)
    logger.debug("smtp_username: %s", smtp_username)
    logger.debug("smtp_password: %s", smtp_password)
    logger.debug("smtp_host: %s", smtp_host)
    logger.debug("smtp_port: %s", smtp_port)
    logger.debug("smtp_no_tls: %s", smtp_no_tls)
    logger.debug("notification_email: %s", notification_email)
    logger.debug("log_level: %s", log_level)
    logger.debug("no_progress_bar: %s", no_progress_bar)
    logger.debug("notification_script: %s", notification_script)
    logger.debug("unverified_https: %s", unverified_https)

    # check required directory param only if not list albums
    if not list_albums and not directory:
        print('--directory or --list-albums are required')
        sys.exit(constants.ExitCode.EXIT_FAILED_MISSING_COMMAND.value)

    def print_duplicates(duplicates):
        if duplicates:
            duplicate_iter = iter(duplicates)
            size = 0
            while True:
                try:
                    duplicate = next(duplicate_iter)
                    logger.info("there are %s duplicates with md5 %s and size %s:",
                                duplicate['count'], duplicate['md5'], duplicate['size'])
                    count = duplicate['count']
                    for i in range(0, count):
                        logger.info("duplicate:%s: %s", duplicate['md5'], duplicate['path'])
                        if i < count - 1:
                            size = size + int(duplicate['size'])
                            duplicate = next(duplicate_iter)

                except StopIteration:
                    if size > 1024*1024*1024:
                        logger.info("%.1f GB could be reclaimed", size/(1024*1024*1024))
                    elif size > 1024*1024:
                        logger.info("%.1f MB could be reclaimed", size/(1024*1024))
                    elif size > 1024:
                        logger.info("%.1f KB could be reclaimed", size/(1024))
                    else:
                        logger.info("%d bytes could be reclaimed", size)
                    break
        else:
            logger.info("there are no duplicates")

    if not username and directory and list_duplicates:
        print_duplicates(mdb.fetch_duplicates())
        sys.exit(constants.ExitCode.EXIT_NORMAL.value)

    raise_authorization_exception = (
        smtp_username is not None
        or notification_email is not None
        or notification_script is not None
        or not sys.stdout.isatty()
    )

    try:
        logger.debug("connecting to iCloudService...")
        #icloud = PyiCloudService(
        icloud = authenticate(
            username,
            password,
            cookie_directory,
            raise_authorization_exception,
            client_id=os.environ.get("CLIENT_ID"),
            unverified_https=unverified_https)
    except PyiCloud2SARequiredException as ex:
        if notification_script is not None:
            logger.debug("executing notification script %s", notification_script)
            subprocess.call([notification_script])
        if smtp_username is not None or notification_email is not None:
            logger.debug("sending 2sa email notification")
            send_2sa_notification(
                smtp_username,
                smtp_password,
                smtp_host,
                smtp_port,
                smtp_no_tls,
                notification_email,
            )
        logger.error(ex)
        sys.exit(constants.ExitCode.EXIT_FAILED_2FA_REQUIRED.value)
    except PyiCloudFailedLoginException as ex:
        logger.error(ex)
        sys.exit(constants.ExitCode.EXIT_FAILED_LOGIN.value)

    # Default album is "All Photos", so this is the same as
    # calling `icloud.photos.all`.
    # After 6 or 7 runs within 1h Apple blocks the API for some time. In that
    # case exit.
    try:
        logger.info("fetching library information from iCloudService...")
        photos = icloud.photos.all
    except PyiCloudAPIResponseException as ex:
        # For later: come up with a nicer message to the user. For now take the
        # exception text
        print(ex)
        sys.exit(constants.ExitCode.EXIT_FAILED_CLOUD_API.value)

    albums = icloud.photos.albums
    logger.info("there are %d assets in %d albums in your library", len(photos), len(albums))

    album_titles = [str(a) for a in albums]
    smart_album_titles = [str(a) for a in albums if  isinstance(a, SmartPhotoAlbum)]
    album_titles.sort(reverse = sort=='desc')
    if list_albums:
        if skip_smart_folders:
            album_titles = [_ for _ in album_titles if _ in smart_album_titles] #.keys()
        print(*album_titles, sep="\n")
        sys.exit(constants.ExitCode.EXIT_NORMAL.value)

    directory = os.path.normpath(directory)
    newest_created = datetime.datetime.fromtimestamp(0).astimezone(get_localzone())
    newest_name = "unknown"
    logger.info("setting newest asset date to %s and newest asset name to %s",
                newest_created, newest_name)

    if date_since is not None:
        date_since = date_since.astimezone(get_localzone())
        logger.info("assets older than %s will be skipped (from date-since)", date_since)

    if newest:
        # (filename, created)
        newest_asset = mdb.newest_asset()
        if newest_asset is not None:
            newest_created = newest_asset["created"].astimezone(get_localzone())
            date_since = newest_created
            newest_asset = newest_asset["path"]
            logger.info("setting newest asset date to %s and newest asset name to %s",
                        newest_created, newest_asset)
            logger.info("assets older than %s will be skipped (from database)", date_since)
        else:
            logger.info("newest asset date not found in database")

    def photos_exception_handler(ex, retries):
        """Handles session errors in the PhotoAlbum photos iterator"""
        nonlocal icloud
        if "Invalid global session" in str(ex):
            if retries > constants.DOWNLOAD_MEDIA_MAX_RETRIES:
                logger.info("iCloud re-authentication failed! Please try again later.")
                raise ex
            logger.error("Session error, re-authenticating...")
            if retries > 1:
                # If the first reauthentication attempt failed,
                # start waiting a few seconds before retrying in case
                # there are some issues with the Apple servers
                time.sleep(constants.DOWNLOAD_MEDIA_RETRY_CONNECTION_WAIT_SECONDS * retries)
            icloud = authenticate(username, password)
        else:
            if retries > constants.DOWNLOAD_MEDIA_MAX_RETRIES:
                logger.error("photos_exception_handler: giving up: %s", ex)
                raise ex
            logger.error("photos_exception_handler: retrying: %s", ex)
            time.sleep(constants.DOWNLOAD_MEDIA_RETRY_CONNECTION_WAIT_SECONDS)

    def download_album(album):
        def download_photo(photo):
            """internal function for actually downloading the photos"""

            nonlocal reached_date_since
            nonlocal consecutive_files_found
            pdb = database.DatabaseHandler()
            photo_metadata = None

            if skip_videos and photo.item_type != "image":
                logger.info("%s: skipping %s, only downloading photos.",
                                            album, photo.filename)
                return None
            if photo.item_type not in ["image", "movie"]:
                logger.info("%s: skipping %s, only downloading"
                                            " photos and videos. (Item type was: %s)",
                                            album, photo.filename, photo.item_type)
                return None

            try:
                created_date = photo.created.astimezone(get_localzone())
            except (ValueError, OSError):
                logger.error("%s: Could not convert photo %s "
                                            "created date to local timezone (%s)",
                                            album, photo.filename, photo.created)
                created_date = photo.created

            download_dir = build_download_dir(directory, folder_structure, album,
                                              created_date, datetime.datetime.fromtimestamp(0))
            download_size = size

            try:
                versions = photo.versions
            except KeyError as ex:
                print(f"KeyError: {ex} attribute was not found in the photo fields!")
                with open("icloudpd-photo-error.json", "w", encoding="utf-8") as outfile:
                    # pylint: disable=protected-access
                    json.dump({
                        "master_record": photo._master_record,
                        "asset_record": photo._asset_record
                    }, outfile)
                    # pylint: enable=protected-access
                print("icloudpd has saved the photo record to: "
                    "./icloudpd-photo-error.json")
                print("Please create a Gist with the contents of this file: "
                    "https://gist.github.com")
                print(
                    "Then create an issue on GitHub: "
                    "https://github.com/icloud-photos-downloader/icloud_photos_downloader/issues")
                print(
                    "Include a link to the Gist in your issue, so that we can "
                    "see what went wrong.\n")
                return None

            if size not in versions and size != "original":
                if force_size:
                    filename = photo.filename.encode("utf-8").decode("ascii", "ignore")
                    logger.error("%s: %s size does not exist for %s. skipping...",
                                                album, size, filename)
                    return None
                download_size = "original"

            if date_since is not None:
                if created_date < date_since:
                    logger.debug("%s: reached date since %s on %s dated %s",
                                 album, date_since, created_date,
                                 photo.filename.encode('utf-8').decode('ascii', 'ignore'))
                    reached_date_since = True
                    return None

            download_path = local_download_path(photo, download_size, download_dir)
            short_path = download_path[len(directory)+1:]

            file_exists = os.path.isfile(download_path)
            if not file_exists and download_size == "original":
                # Deprecation - We used to download files like IMG_1234-original.jpg,
                # so we need to check for these.
                # Now we match the behavior of iCloud for Windows: IMG_1234.jpg
                original_download_path = f"-{size}.".join(download_path.rsplit(".", 1))
                #original_download_path = ("-%s." % size).join(download_path.rsplit(".", 1))
                file_exists = os.path.isfile(original_download_path)

            if file_exists:
                # for later: this crashes if download-size medium is specified
                file_size = os.stat(download_path).st_size
                version = photo.versions[download_size]

                photo_size = version["size"]
                if file_size != photo_size:
                    download_path = f"-{photo_size}.".join(download_path.rsplit(".", 1))
                    short_path = download_path[len(directory)+1:]
                    logger.info("%s: deduplicated (size) %s file size %s photo size %s dated %s",
                                album, truncate_middle(short_path, 96),
                                file_size, photo_size, created_date)
                    file_exists = os.path.isfile(download_path)
                if file_exists:
                    consecutive_files_found = consecutive_files_found + 1
                    logger.info("%s: skipping (already exists) %s dated %s",
                                album, truncate_middle(short_path, 96),
                                created_date)
                    if not pdb.asset_exists(short_path):
                        md5 = calculate_md5(download_path)
                        logger.info("%s: updating %s md5 %s",
                                    album, short_path, md5)
                    else:
                        md5 = pdb.get_asset_md5(short_path)
                    photo_metadata = get_photo_metadata(photo, album, short_path, md5)
                    photo_metadata['file_size'] = os.stat(download_path).st_size
                    # Check for multiple occurrences of same asset in
                    # iCloud Photos library (happened with WhatsApp)

            if not file_exists:
                consecutive_files_found = 0
                if only_print_filenames:
                    print(download_path)
                    photo_metadata = get_photo_metadata(photo, album,
                                                        short_path, -1)
                else:
                    logger.info("%s: downloading %s dated %s",
                                album,
                                truncate_middle(short_path, 96),
                                created_date)
                    download_result = download.download_media(icloud, photo,
                                                              download_path, download_size)
                    if download_result:
                        if (set_exif_datetime and photo.filename.lower().endswith((".jpg", ".jpeg"))
                            and not get_photo_exif(download_path)):
                            # %Y:%m:%d looks wrong but it's the correct format
                            date_str = created_date.strftime("%Y-%m-%d %H:%M:%S%z")
                            logger.debug("%s: setting EXIF timestamp for : %s %s",
                                         album, short_path, date_str)
                            set_photo_exif(download_path,
                                                         created_date.strftime("%Y:%m:%d %H:%M:%S"))
                        download.set_utime(download_path, created_date)
                        md5 = calculate_md5(download_path)
                        photo_metadata = get_photo_metadata(photo, album, short_path, md5)
                        photo_metadata['file_size'] = os.stat(download_path).st_size


            # Also download the live photo if present
            if not skip_live_photos:
                lp_size = live_photo_size + "Video"
                if lp_size in photo.versions:
                    version = photo.versions[lp_size]
                    filename = version["filename"]
                    if live_photo_size != "original":
                        # Add size to filename if not original
                        filename = filename.replace(".MOV", f"-{live_photo_size}.MOV")
                    lp_download_path = os.path.join(download_dir, filename)
                    lp_short_path = lp_download_path[len(directory)+1:]
                    lp_file_exists = os.path.isfile(lp_download_path)
                    if only_print_filenames and not lp_file_exists:
                        print(lp_download_path)
                        photo_metadata = get_photo_metadata(photo, album,
                                                            lp_short_path, -1)
                    else:
                        if lp_file_exists:
                            lp_file_size = os.stat(lp_download_path).st_size
                            lp_photo_size = version["size"]
                            if lp_file_size != lp_photo_size:
                                lp_download_path = f"-{lp_photo_size}.".join(
                                    lp_download_path.rsplit(".", 1))
                                logger.info("%s: deduplicated (live) %s file size %s"
                                            " photo size %s dated %s",
                                            album,
                                            truncate_middle(
                                                lp_short_path, 96),
                                            lp_file_size, lp_photo_size, created_date)
                                lp_file_exists = os.path.isfile(lp_download_path)
                            if lp_file_exists:
                                logger.info("%s: skipping (already exists) %s dated %s",
                                            album,
                                            truncate_middle(
                                                lp_short_path, 96),
                                            created_date)
                                if not pdb.asset_exists(lp_short_path):
                                    md5 = calculate_md5(lp_download_path)
                                    logger.info("%s: updating %s md5 %s",
                                                album, lp_download_path, md5)
                                else:
                                    md5 = pdb.get_asset_md5(lp_short_path)

                                photo_metadata = get_photo_metadata(photo,
                                                                    album, lp_short_path, md5)
                                photo_metadata['file_size'] = os.stat(lp_download_path).st_size
                        if not lp_file_exists:
                            logger.info("%s: downloading %s dated %s",
                                        album,
                                        truncate_middle(lp_short_path, 96),
                                        created_date)
                            download.download_media(icloud, photo, lp_download_path, lp_size)
                            md5 = calculate_md5(lp_download_path)

                            photo_metadata = get_photo_metadata(photo, album, lp_short_path, md5)
                            photo_metadata['file_size'] = os.stat(lp_download_path).st_size

            if photo_metadata is None:
                pass
            return photo_metadata

        amd = {}
        amd['album_name'] = album
        amd['assets'] = []
        photos = icloud.photos.albums.find(album)
        photos.exception_handler = photos_exception_handler
        photos_count = len(photos)

        # Optional: Only download the x most recent photos.
        if recent is not None:
            photos_count = recent
            photos = itertools.islice(photos, recent)

        if until_found is not None:
            photos_count = "???"
            # ensure photos iterator doesn't have a known length
            photos = (p for p in photos)

        plural_suffix = "" if photos_count == 1 else "s"
        video_suffix = ""
        if not skip_videos:
            video_suffix = " or video" if photos_count == 1 else " and videos"
        logger.info("%s: processing %s %s photo%s%s",
                    album, photos_count, size, plural_suffix, video_suffix)

        consecutive_files_found = 0
        reached_date_since = False

        pending = set()
        with ThreadPoolExecutor() as tpe:
            for photo in iter(photos):
                if (until_found is not None
                    and consecutive_files_found >= until_found) or reached_date_since:
                    if reached_date_since:
                        logger.info("%s: processed all assets more recent than %s",
                                        album, date_since)
                    else:
                        logger.info("%s: found %s consecutive previously downloaded photos",
                                        album, until_found)
                    break

                future = tpe.submit(download_photo, photo)
                pending = pending | set([future])
            while pending:
                done, pending = as_completed(pending), set()
                for future in done:
                    amd['assets'].append(future.result())

        if not reached_date_since:
            logger.info("%s: processed all assets", album)

        return amd

    cmd = ThreadSafeDict()
    cmd['icloud_username'] = username
    cmd['directory'] = directory
    cmd['albums'] = ThreadSafeDict()
    if all_albums:
        if skip_all_photos:
            logger.info("removing All Photos from the list of albums to process")
            album_titles = [album for album in album_titles if album != "Library"]
        if skip_smart_folders:
            logger.info("removing smart folders from the list of albums to process")
            album_titles = [
                album for album in album_titles
                if album not in smart_album_titles]
    else:
        album_titles = [album]

    logger.info("the following albums will be processed:")
    for title in album_titles:
        logger.info(title)

    pending = set()
    with ThreadPoolExecutor(max_workers=8) as tpe:
        for title in album_titles:
            future = tpe.submit(download_album, title)
            pending = pending | set([future])
        while pending:
            done, pending = as_completed(pending), set()
            for future in done:
                amd = future.result()
                cmd['albums'][amd['album_name']] = amd

    for album_name, amd in cmd['albums'].items():
        for pmd in amd['assets']:
            photo = pmd['photo']
            path = pmd['path']
            md5 = pmd['md5']
            if not mdb.asset_exists(path):
                logger.info("upsert %s %s %s", album_name, path, md5)
                mdb.upsert_asset(album_name, photo, path, md5)
            del pmd['photo']

    if auto_delete:
        autodelete_photos(icloud, folder_structure, directory)

    if create_json_listing:
        json_file_path = directory + "/" + "catalog.json"
        logger.info("writing json listing to %s", json_file_path)
        with open(json_file_path, "w", encoding="utf-8") as jsonfile:
            jsonfile.write(json.dumps(thread_safe_dict_to_dict_recursive(cmd), indent=4))

    if list_duplicates:
        print_duplicates(mdb.fetch_duplicates())

    newest_asset = mdb.newest_asset()
    logger.info("Most recent asset in library is %s dated %s",
                newest_asset['path'], newest_asset['created'])
    logger.info("completed in %s", datetime.datetime.now() - start)

if __name__ == "__main__":
    main()
