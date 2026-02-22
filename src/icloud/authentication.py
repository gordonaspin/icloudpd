"""Handles username/password authentication and two-step authentication"""
import sys
import logging
from logging import Logger
from typing import Any

import click

from pyicloud import PyiCloudService
from pyicloud import utils
from pyicloud.exceptions import (
    PyiCloud2SARequiredException,
    PyiCloudFailedLoginException,
    PyiCloudNoStoredPasswordAvailableException)

import constants

logger: Logger = logging.getLogger(__name__)

def _handle_2fa(api: PyiCloudService) -> None:
    # fmt: off
    print("\ntwo-factor (2FA) authentication required.")
    # fmt: on
    code = input("\nPlease enter verification code: ")
    if not api.validate_2fa_code(code):
        logger.debug("failed to verify (2FA) verification code")
        sys.exit(constants.ExitCode.EXIT_FAILED_VERIFY_2FA_CODE.value)

def _handle_2sa(api: PyiCloudService) -> None:
    # fmt: off
    print("\ntwo-step (2SA) authentication required.")
    # fmt: on
    print("\nyour trusted devices are:")
    devices: list[dict[str, Any]] = api.trusted_devices
    device: Any = None
    while device is None:
        for i, device in enumerate(devices):
            phone = device.get("phoneNumber")
            name = device.get("deviceName", f"SMS to {phone}")
            print(f"{i}: {name}")

        device_index = int(input("\nwhich device number would you like to use: "))
        device = devices.get(device_index, None)
        if device is None:
            print("invalid device chosen, please retry")
        else:
            break

    if not api.send_verification_code(device):
        logger.debug("failed to send verification code")
        sys.exit(constants.ExitCode.EXIT_FAILED_SEND_2SA_CODE.value)

    code = input("\nPlease enter two-step (2SA) validation code: ")
    if not api.validate_verification_code(device, code):
        print("failed to verify verification code")
        sys.exit(constants.ExitCode.EXIT_FAILED_VERIFY_2FA_CODE.value)


def authenticate(
    username: str,
    password: str,
    cookie_directory: str = None,
    raise_authorization_exception: bool = False,
    client_id: str = None,
    unverified_https: bool=False
) -> PyiCloudService:
    """Authenticate with iCloud username and password"""
    logger.debug("authenticating")
    failure_count = 0

    while True:
        try:
            api: PyiCloudService = PyiCloudService(
                apple_id=username,
                password=password,
                cookie_directory=cookie_directory,
                client_id=client_id,
                verify=not unverified_https)

            if api.requires_2fa:
                if raise_authorization_exception:
                    raise PyiCloud2SARequiredException(username)
                _handle_2fa(api)

            elif api.requires_2sa:
                if raise_authorization_exception:
                    raise PyiCloud2SARequiredException(username)
                _handle_2sa(api)
            # Auth success
            logger.debug("authenticated as %s", username)
            return api

        except PyiCloudFailedLoginException as e:
            failure_count += 1
            message = (f"PyiCloudFailedLoginException for {username}, {e}, "
                      f"failure count {failure_count}")
            logger.info(message)
            if failure_count >= constants.AUTHENTICATION_MAX_RETRIES:
                raise PyiCloudFailedLoginException(message) from e


        except PyiCloudNoStoredPasswordAvailableException as e:
            if raise_authorization_exception:
                message = f"no stored password available for {username} and not a TTY!"
                raise PyiCloudFailedLoginException(message) from e

            # Prompt for password if not stored in PyiCloud's keyring
            password = click.prompt("iCloud password", hide_input=True)
            if (
                not utils.password_exists_in_keyring(username)
                and click.confirm("save password in keyring?")
            ):
                utils.store_password_in_keyring(username, password)
