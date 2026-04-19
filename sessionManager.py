import atexit
import glob
import logging
import os
import socket
import time
from os import getenv, path
from telethon.sessions import StringSession

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows fallback
    fcntl = None

TELEGRAM_DAEMON_SESSION_PATH = getenv("TELEGRAM_DAEMON_SESSION_PATH")
TELEGRAM_DAEMON_LOCK_FILE = getenv("TELEGRAM_DAEMON_LOCK_FILE")
sessionName = "DownloadDaemon"
stringSessionFilename = "{0}.session".format(sessionName)
lockFilename = "{0}.lock".format(sessionName)
logger = logging.getLogger('telegram-download-daemon.session')
_lockHandle = None
_resolvedLockPath = None


class SingleInstanceLockError(RuntimeError):
    pass


def _getSessionPath():
    if not TELEGRAM_DAEMON_SESSION_PATH:
        return None
    os.makedirs(TELEGRAM_DAEMON_SESSION_PATH, exist_ok=True)
    return path.join(TELEGRAM_DAEMON_SESSION_PATH, stringSessionFilename)


def _isDirectoryWritable(directory):
    try:
        os.makedirs(directory, exist_ok=True)
        probe_path = path.join(directory, f".{lockFilename}.probe.{os.getpid()}")
        with open(probe_path, 'a', encoding='utf-8'):
            pass
        os.remove(probe_path)
        return True, None
    except OSError as exc:
        return False, exc


def _getLockPath():
    global _resolvedLockPath

    if _resolvedLockPath is not None:
        return _resolvedLockPath

    if TELEGRAM_DAEMON_LOCK_FILE:
        lock_dir = path.dirname(TELEGRAM_DAEMON_LOCK_FILE)
        if lock_dir:
            os.makedirs(lock_dir, exist_ok=True)
        _resolvedLockPath = TELEGRAM_DAEMON_LOCK_FILE
        return _resolvedLockPath

    if TELEGRAM_DAEMON_SESSION_PATH:
        writable, exc = _isDirectoryWritable(TELEGRAM_DAEMON_SESSION_PATH)
        if writable:
            _resolvedLockPath = path.join(TELEGRAM_DAEMON_SESSION_PATH, lockFilename)
            return _resolvedLockPath
        logger.warning(
            "Session directory %s is not writable for lock files (%s); "
            "falling back to /tmp. Set TELEGRAM_DAEMON_LOCK_FILE to override.",
            TELEGRAM_DAEMON_SESSION_PATH,
            exc,
        )

    _resolvedLockPath = path.join("/tmp", lockFilename)
    return _resolvedLockPath


def getLockPath():
    return _getLockPath()


def archiveSessionArtifacts(reason="invalid"):
    timestamp = time.strftime('%Y%m%d-%H%M%S')
    archived_paths = []

    if TELEGRAM_DAEMON_SESSION_PATH:
        sessionPath = _getSessionPath()
        if sessionPath and path.exists(sessionPath):
            archivedPath = f"{sessionPath}.{reason}.{timestamp}"
            os.replace(sessionPath, archivedPath)
            archived_paths.append(archivedPath)
        return archived_paths

    session_prefix = f"{sessionName}.session"
    session_artifacts = sorted(glob.glob(f"{session_prefix}*"))
    for session_artifact in session_artifacts:
        suffix = session_artifact[len(session_prefix):]
        archived_path = f"{session_prefix}.{reason}.{timestamp}{suffix}"
        os.replace(session_artifact, archived_path)
        archived_paths.append(archived_path)

    return archived_paths


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


def acquireProcessLock():
    global _lockHandle

    if _lockHandle is not None:
        return _lockHandle

    lockPath = _getLockPath()
    lockHandle = open(lockPath, 'a+', encoding='utf-8')

    if fcntl is None:
        logger.warning("fcntl is unavailable on this platform; single-instance lock is disabled")
        _lockHandle = lockHandle
        return _lockHandle

    try:
        fcntl.flock(lockHandle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        lockHandle.seek(0)
        owner = lockHandle.read().strip()
        lockHandle.close()
        owner_text = f" Lock owner: {owner}" if owner else ""
        raise SingleInstanceLockError(
            f"Another telegram-download-daemon instance is already using this session lock: {lockPath}.{owner_text}"
        )

    lockHandle.seek(0)
    lockHandle.truncate()
    lockHandle.write(
        "pid={pid} host={host} started_at={started_at}\n".format(
            pid=os.getpid(),
            host=socket.gethostname(),
            started_at=time.strftime('%Y-%m-%d %H:%M:%S'),
        )
    )
    lockHandle.flush()
    os.fsync(lockHandle.fileno())
    _lockHandle = lockHandle
    atexit.register(releaseProcessLock)
    logger.info("Single-instance lock acquired: %s", lockPath)
    return _lockHandle


def releaseProcessLock():
    global _lockHandle

    if _lockHandle is None:
        return

    try:
        if fcntl is not None:
            fcntl.flock(_lockHandle.fileno(), fcntl.LOCK_UN)
    except OSError:
        pass
    finally:
        try:
            _lockHandle.close()
        except OSError:
            pass
        _lockHandle = None
