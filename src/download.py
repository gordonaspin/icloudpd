"""Handles file downloads with retries and error handling"""

import os
import socket
import time
import logging
from tzlocal import get_localzone
from requests.exceptions import ConnectionError  # pylint: disable=redefined-builtin
from pyicloud.exceptions import PyiCloudAPIResponseException
# Import the constants object so that we can mock WAIT_SECONDS in tests
import constants


def update_mtime(photo, download_path):
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
        set_utime(download_path, created_date)

def set_utime(download_path, created_date):
    """Set date & time of the file"""
    ctime = time.mktime(created_date.timetuple())
    os.utime(download_path, (ctime, ctime))

def download_media(icloud, photo, download_path, size):
    """Download the photo to path, with retries and error handling"""
    logger = logging.getLogger("icloudpd")

    # get back the directory for the file to be downloaded and create it if not there already
    download_dir = os.path.dirname(download_path)

    if not os.path.exists(download_dir):
        try:
            os.makedirs(download_dir)
        except OSError:  # pragma: no cover
            pass         # pragma: no cover

    for retries in range(constants.DOWNLOAD_MEDIA_MAX_RETRIES):
        try:
            photo_data = photo.download(size)
            if photo_data:
                temp_download_path = download_path + ".part"
                with open(temp_download_path, "wb") as file_obj:
                    file_obj.write(photo_data)

                os.rename(temp_download_path, download_path)
                update_mtime(photo, download_path)
                return True

            logger.error("Could not find URL to download %s for size %s!",
                photo.filename, size)
            break

        except (ConnectionError, socket.timeout, PyiCloudAPIResponseException) as ex:
            if "Invalid global session" in str(ex):
                logger.error("Session error, re-authenticating...")
                if retries > 0:
                    # If the first reauthentication attempt failed,
                    # start waiting a few seconds before retrying in case
                    # there are some issues with the Apple servers
                    time.sleep(constants.DOWNLOAD_MEDIA_RETRY_CONNECTION_WAIT_SECONDS)

                icloud.authenticate()
            else:
                # you end up here when p.e. throttleing by Apple happens
                wait_time = (retries + 1) * constants.DOWNLOAD_MEDIA_RETRY_CONNECTION_WAIT_SECONDS
                logger.error("Error %s downloading %s, retrying after %d seconds...",
                             ex, photo.filename, wait_time)
                time.sleep(wait_time)

        except IOError:
            logger.error(
                "IOError while writing file to %s! "
                "You might have run out of disk space, or the file "
                "might be too large for your OS. "
                "Skipping this file...", download_path
            )
            break
    else:
        logger.warning("Could not download %s! Please try again later.", photo.filename)

    return False
