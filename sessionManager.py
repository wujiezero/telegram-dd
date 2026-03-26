import logging
import os
from os import getenv, path
from telethon.sessions import StringSession

TELEGRAM_DAEMON_SESSION_PATH = getenv("TELEGRAM_DAEMON_SESSION_PATH")
sessionName = "DownloadDaemon"
stringSessionFilename = "{0}.session".format(sessionName)
logger = logging.getLogger('telegram-download-daemon.session')


def _getSessionPath():
    if not TELEGRAM_DAEMON_SESSION_PATH:
        return None
    os.makedirs(TELEGRAM_DAEMON_SESSION_PATH, exist_ok=True)
    return path.join(TELEGRAM_DAEMON_SESSION_PATH, stringSessionFilename)


def _getStringSessionIfExists():
    sessionPath = _getSessionPath()
    if sessionPath and path.isfile(sessionPath):
        try:
            with open(sessionPath, 'r', encoding='utf-8') as file:
                session = file.read().strip()
                if session:
                    logger.info("Session loaded from %s", sessionPath)
                    return session
                logger.warning("Session file is empty: %s", sessionPath)
        except OSError as exc:
            logger.error("Failed to read session file %s: %s", sessionPath, exc)
    return None


def getSession():
    if not TELEGRAM_DAEMON_SESSION_PATH:
        return sessionName

    return StringSession(_getStringSessionIfExists())


def saveSession(session):
    sessionPath = _getSessionPath()
    if sessionPath:
        sessionData = StringSession.save(session)
        tempPath = f"{sessionPath}.tmp"
        try:
            with open(tempPath, 'w', encoding='utf-8') as file:
                file.write(sessionData)
                file.flush()
                os.fsync(file.fileno())
            os.replace(tempPath, sessionPath)
            logger.info("Session saved in %s", sessionPath)
        except OSError as exc:
            logger.error("Failed to save session in %s: %s", sessionPath, exc)
            try:
                if path.exists(tempPath):
                    os.remove(tempPath)
            except OSError:
                pass
