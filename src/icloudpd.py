"""Main script that uses Click to parse command-line arguments"""
from __future__ import print_function

from datetime import datetime
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from tzlocal import get_localzone

import click
import urllib3
from urllib3.exceptions import InsecureRequestWarning

from pyicloud.services.photos import SmartAlbumEnum
import constants
from context import Context
from database import database
from photo_manager import PhotoManager
from logger.logger import setup_logging

logger = logging.getLogger("icloudpd")

urllib3.disable_warnings(category=InsecureRequestWarning)

def print_duplicates(duplicates):
    """print dupes"""
    if duplicates:
        duplicate_iter = iter(duplicates)
        size = 0
        while True:
            try:
                duplicate = next(duplicate_iter)
                print(f"there are {duplicate['count']} duplicates"
                      " with md5 {duplicate['md5']} and size {duplicate['size']}:")
                count = duplicate['count']
                for i in range(0, count):
                    print(f"duplicate:{duplicate['md5']}: {duplicate['path']}")
                    if i < count - 1:
                        size = size + int(duplicate['size'])
                        duplicate = next(duplicate_iter)

            except StopIteration:
                if size > 1024*1024*1024:
                    print(f"{size/(1024*1024*1024):.1f} GB could be reclaimed")
                elif size > 1024*1024:
                    print(f"{size/(1024*1024):.1f} MB could be reclaimed")
                elif size > 1024:
                    print(f"{size/(1024):.1f} KB could be reclaimed")
                else:
                    print(f"{size} bytes could be reclaimed")
                break
    else:
        print("there are no duplicates")

@click.command(
        context_settings={"help_option_names": ["-h", "--help"]},
        options_metavar="<options>",
        no_args_is_help=True)
@click.option("-d", "--directory",
              required=True,
              help="Local directory that should be used for download",
              type=click.Path(exists=True),
              metavar="<directory>")
@click.option("-u", "--username",
              required=True,
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
@click.option("-a", "--album",
              help=f"Album to download (default: {SmartAlbumEnum.ALL_PHOTOS.value})",
              metavar="<album>",
              default=SmartAlbumEnum.ALL_PHOTOS.value)
@click.option("--all-albums",
              help="Download all albums",
              is_flag=True)
@click.option("--skip-smart-folders",
              help="Exclude smart folders from listing or download: " +
              f"{", ".join(i.value for i in SmartAlbumEnum)}",
              is_flag=True)
@click.option("--skip-Library",
              help=f"Exclude the smart folder {SmartAlbumEnum.ALL_PHOTOS.value}"
              " from listing or download",
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
@click.option("--logging-config",
              help="JSON logging config filename (default: logging-config.json)",
              metavar="<filename>",
              default="logging-config.json",
              show_default=True)
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
        album,
        all_albums,
        skip_smart_folders,
        skip_library,
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
        notification_script,
        logging_config,
        unverified_https,
):
    """Download all iCloud photos to a local directory"""

    start = datetime.now()
    setup_logging(logging_config=logging_config)

    if date_since is not None:
        date_since = date_since.astimezone(get_localzone())
        logger.info("assets older than %s will be skipped (from date-since)", date_since)

    ctx: Context = Context(directory=directory, username=username, password=password,
                    cookie_directory=cookie_directory, size=size,
                    live_photo_size=live_photo_size, recent=recent, date_since=date_since,
                    newest=newest, album=album, all_albums=all_albums,
                    skip_smart_folders=skip_smart_folders,
                    skip_all_photos=skip_library, list_albums=list_albums, sort=sort,
                    skip_videos=skip_videos, skip_live_photos=skip_live_photos,
                    force_size=force_size, auto_delete=auto_delete,
                    only_print_filenames=only_print_filenames, folder_structure=folder_structure,
                    list_duplicates=list_duplicates, create_json_listing=create_json_listing,
                    set_exif_datetime=set_exif_datetime, smtp_username=smtp_username,
                    smtp_password=smtp_password, smtp_host=smtp_host, smtp_port=smtp_port,
                    smtp_no_tls=smtp_no_tls, notification_email=notification_email,
                    notification_script=notification_script, logging_config=logging_config,
                    unverified_https=unverified_https)

    logger.debug("directory: %s", ctx.directory)
    logger.debug("username: %s", ctx.username)
    logger.debug("cookie_directory: %s", ctx.cookie_directory)
    logger.debug("size: %s", ctx.size)
    logger.debug("live_photo_size %s", ctx.live_photo_size)
    logger.debug("recent: %s", ctx.recent)
    logger.debug("date_since: %s", ctx.date_since)
    logger.debug("newest: %s", ctx.newest)
    logger.debug("album: %s", ctx.album)
    logger.debug("all_albums: %s", ctx.all_albums)
    logger.debug("skip_smart_folders: %s", ctx.skip_smart_folders)
    logger.debug("skip_all_photos: %s", ctx.skip_all_photos)
    logger.debug("list_albums: %s", ctx.list_albums)
    logger.debug("sort: %s", ctx.sort)
    logger.debug("skip_videos: %s", ctx.skip_videos)
    logger.debug("skip_live_photos: %s", ctx.skip_live_photos)
    logger.debug("force_size: %s", ctx.force_size)
    logger.debug("auto_delete: %s", ctx.auto_delete)
    logger.debug("only_print_filenames: %s", ctx.only_print_filenames)
    logger.debug("folder_structure: %s", ctx.folder_structure)
    logger.debug("list_duplicates: %s", ctx.list_duplicates)
    logger.debug("set_exif_datetime: %s", ctx.set_exif_datetime)
    logger.debug("smtp_username: %s", ctx.smtp_username)
    logger.debug("smtp_password: %s", ctx.smtp_password)
    logger.debug("smtp_host: %s", ctx.smtp_host)
    logger.debug("smtp_port: %s", ctx.smtp_port)
    logger.debug("smtp_no_tls: %s", ctx.smtp_no_tls)
    logger.debug("notification_email: %s", ctx.notification_email)
    logger.debug("notification_script: %s", ctx.notification_script)
    logger.debug("logging_config: %s", ctx.logging_config)
    logger.debug("unverified_https: %s", ctx.unverified_https)

    mdb = None

    # check required directory param only if not list albums
    if not (list_albums or only_print_filenames) and not directory:
        print('--directory or --list-albums are required')
        sys.exit(constants.ExitCode.EXIT_FAILED_MISSING_COMMAND.value)

    if directory:
        database.setup_database(directory)
        #setup_database_logger()
        mdb = database.DatabaseHandler()

    if not username and directory and list_duplicates:
        print_duplicates(mdb.fetch_duplicates())
        sys.exit(constants.ExitCode.EXIT_NORMAL.value)

    if list_albums or list_duplicates or only_print_filenames:
        logger.disabled = True

    pm : PhotoManager = PhotoManager(ctx=ctx)

    logger.info("there are %d assets in %d albums in your library",
                pm.photos_count(), pm.albums_count())

    album_titles = pm.album_names()
    smart_album_titles = pm.smart_album_names()

    album_titles.sort(reverse = sort=='desc')
    if list_albums:
        if skip_smart_folders:
            album_titles = [_ for _ in album_titles if _ not in smart_album_titles] #.keys()
        print(*album_titles, sep="\n")
        sys.exit(constants.ExitCode.EXIT_NORMAL.value)

    newest_created = datetime.fromtimestamp(0).astimezone(get_localzone())
    newest_name = "unknown"
    logger.info("setting newest asset date to %s and newest asset name to %s",
                newest_created, newest_name)

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

    cmd = {}
    cmd['icloud_username'] = username
    cmd['directory'] = directory
    cmd['date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cmd['albums'] = {}
    if all_albums:
        if skip_library:
            logger.info("removing %s from the list of albums to process",
                        SmartAlbumEnum.ALL_PHOTOS.value)
            album_titles = [a for a in album_titles if a != SmartAlbumEnum.ALL_PHOTOS.value]
        if skip_smart_folders:
            logger.info("removing smart folders from the list of albums to process")
            album_titles = [
                _ for _ in album_titles
                if _ not in smart_album_titles]
    else:
        album_titles = [album]

    logger.info("the following albums will be processed:")
    for title in album_titles:
        logger.info(title)

    pending = set()
    with ThreadPoolExecutor(max_workers=8) as tpe:
        for title in album_titles:
            future = tpe.submit(pm.download_album, title)
            pending = pending | set([future])
        while pending:
            done, pending = as_completed(pending), set()
            for future in done:
                amd = future.result()
                cmd['albums'][amd.name] = amd

    for album_name, amd in cmd['albums'].items():
        for pmd in amd.assets:
            photo = pmd.photo
            path = pmd.path
            md5 = pmd.md5
            if only_print_filenames:
                print(path)
            if not mdb.asset_exists(path):
                logger.info("upsert %s %s %s", album_name, path, md5)
                mdb.upsert_asset(album_name, photo, path, md5)
            delattr(pmd, 'photo') # remove so we can use json.dumps

    if auto_delete:
        pm.autodelete_photos()

    if create_json_listing and not only_print_filenames:
        json_file_path = directory + "/" + "catalog.json"
        logger.info("writing json listing to %s", json_file_path)
        with open(json_file_path, "w", encoding="utf-8") as jsonfile:
            jsonfile.write(json.dumps(cmd, default=lambda o: o.__dict__, indent=4))

    if list_duplicates:
        print_duplicates(mdb.fetch_duplicates())

    newest_asset = mdb.newest_asset()
    logger.info("Most recent asset in library is %s dated %s",
                newest_asset['path'], newest_asset['created'])
    logger.info("completed in %s", datetime.now() - start)

if __name__ == "__main__":
    main()
