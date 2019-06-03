import contextlib
import json
import logging
import os
import re
import subprocess
import tempfile


class ReleaseException(Exception):
    pass


class ExecException(ReleaseException):
    def __init__(self, exit_code, output):
        super().__init__(json.dumps({
            "error": "execution error",
            "exit_code": exit_code,
            "output": output
        }))


class DataException(ReleaseException):
    def __init__(self, message):
        super().__init__(json.dumps({
            "error": "data error",
            "description": message
        }))


def run(args, cwd):
    p = subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=cwd)
    logging.debug("subprocess: " + " ".join(args))
    output, _ = p.communicate()
    output = output.decode("utf-8")
    logging.debug("output: " + output)
    if p.returncode:
        raise ExecException(p.returncode, output)
    return output


def get_setup_version(repo, branch=None):
    """
    read & validate the version from setup.py

    :param repo: path to local working repo
    :param branch: if defined, first check out this branch
    :return: the version number
    """
    if branch is not None:
        run(["git", "checkout", branch], repo)
    with open(os.path.join(repo, "setup.py")) as f:
        contents = f.read()
    m = re.match(
        ".*version\s*=\s*[\"']([^\"']+)[\"']",
        contents,
        re.DOTALL)
    if m is None:
        raise DataException("unable to find version in setup.py")
    version = m.group(1)
    if re.match("^\d+\.\d+$", version) is None:
        raise DataException(
            "unexpected version format in setup.py: '%s'" % version)
    return version


@contextlib.contextmanager
def cloned_repo(repo_uri):
    """
    makes a new local clone of the repo referred to by uri,
    the clone is automatically deleted when the context is closed

    :param repo_uri: uri of repo to be cloned
    :return: full path of the local clone
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        repo_path = os.path.join(tmpdir, "working")
        run(["git", "clone", repo_uri, repo_path], tmpdir)
        yield repo_path
