from __future__ import annotations

import datetime
import os
import re
from fnmatch import fnmatch
from pathlib import Path
from typing import Literal, Optional

import pygit2
import requests
from more_itertools import first

from .utility import load_json, read_yaml, write_yaml

ts = datetime.datetime.fromtimestamp


def gh_create_url(*, user: str, repository: str, path: str, filename: str, tag: str) -> str:
    if not user or not repository or not filename or not tag:
        raise ValueError("Missing required parameter")
    filename = (filename or "").lstrip("/")
    return f"https://raw.githubusercontent.com/{user}/{repository}/{tag}/{path}/{filename}"


def gh_repository_url(*, user: str, repository: str) -> str:
    return f"https://github.com/{user}/{repository}"


def gh_ls(user: str, repository: str, path: str = "", tag: str = "main", pattern: str | None = None) -> list[dict]:
    url: str = f"https://api.github.com/repos/{user}/{repository}/contents/{path}?ref={tag}"
    data: list[dict] = load_json(url)
    if isinstance(data, dict):
        if 'status' in data:
            raise Exception(f"Github API error: {data.get('message')}")
    if not isinstance(data, list):
        raise Exception(f"Github API error: expected list {url} got {type(data)}")
    if data and pattern is not None:
        data = [x for x in data if fnmatch(x.get("name"), pattern)]
    return data


def gh_tags(folder: str) -> list[str]:
    """Returns tags in given local git repository"""
    repo: pygit2.Repository = pygit2.Repository(path=folder)
    rx: re.Pattern = re.compile(r'^refs/tags/v\d+\.\d+\.\d+$')
    tags: list[str] = sorted([r.removeprefix('refs/tags/') for r in repo.references if rx.match(r)])
    return tags


def gh_get_repo_root(path: str) -> Optional[Path]:
    """
    Return the Path to the top-level Git working directory containing `path`,
    or None if `path` isn’t inside a Git repo.
    """
    try:
        # discover_repository returns the path to the .git folder (or file)
        gitdir = pygit2.discover_repository(path)
    except KeyError:
        # not in a git repository
        return None

    # Build a Repository object from the discovered gitdir
    repo = pygit2.Repository(gitdir)

    # repo.workdir is the absolute path to the working dir (with trailing slash)
    return Path(repo.workdir).resolve()


def gh_get_workdir_ref(path: str) -> tuple[Optional[str], str]:
    """
    Returns a tuple (ref_type, name_or_sha) for the Git workdir at `path`:
      - ('branch', 'main')           if on branch 'main'
      - ('tag',     'v1.2.3')        if HEAD matches tag 'v1.2.3'
      - (None,      'd075ca43')      if detached at commit d075ca43 with no exact tag

    Raises KeyError if `path` isn’t inside a Git repo.
    """
    # 1. Find the .git folder, then open the repo
    gitdir = pygit2.discover_repository(path)
    repo = pygit2.Repository(gitdir)

    # 2. If HEAD is attached to a branch, repo.head_is_detached==False
    if not repo.head_is_detached:
        return "branch", repo.head.shorthand

    # 3. Detached HEAD: look for a tag pointing to this commit
    head_commit = repo.revparse_single("HEAD").id

    for full_ref in repo.references:  # e.g. "refs/tags/v1.2.3", "refs/heads/main", ...
        if not full_ref.startswith("refs/tags/"):
            continue

        ref = repo.references[full_ref]
        # ref.target may point to either:
        #  - a TagObject (annotated tag), whose .target is the underlying commit
        #  - a Commit directly (lightweight tag)
        obj = repo[ref.target]
        target_id = obj.target if isinstance(obj, pygit2.Tag) else obj.id

        if target_id == head_commit:
            tag_name = full_ref.removeprefix("refs/tags/")
            return "tag", tag_name

    # 4. Fallback: detached at a commit that isn’t an exact tag
    return None, head_commit.hex[:8]


class GitInfo:
    def __init__(self, organisation: str, repository: str, source_folder: str):
        self.api_url: str = f"https://api.github.com/repos/{organisation}/{repository}/git"
        self.repository: pygit2.Repository = pygit2.Repository(source_folder)

    @property
    def head(self) -> dict:
        head_ref: pygit2.Reference = self.repository.head
        all_tags: list[pygit2.Reference] = (
            self.repository.references[x] for x in self.repository.references if x.startswith("refs/tags")
        )
        head_tag: pygit2.Reference = first((t for t in all_tags if t.target == head_ref.target), None)
        commit: pygit2.Commit = self.repository[head_ref.target]
        sha: str = commit.hex
        tag: str = head_tag.shorthand if head_tag is not None else ""
        data: dict = {
            "tag": tag,
            "ref": f"refs/tags/{tag}" if tag else "",
            "sha": sha,
            "sha8": sha[:8],
            "tag_url": f"{self.api_url}/{head_tag.name}" if head_tag is not None else "",
            "commit_url": f"{self.api_url}/commits/{sha}",
        }
        return data

    def _local_tag_info(self, tag: str) -> dict:
        if not tag.startswith("refs/tags"):
            tag = f"refs/tags/{tag}"

        if tag not in self.repository.references:
            raise TagNotFoundError(tag)

        tag_object: pygit2.Reference = self.repository.references[tag]
        sha: str = self.repository[tag_object.target].hex
        data: dict = {
            "tag": tag.split("/")[-1],
            "ref": tag_object.name,
            "sha": sha,
            "sha8": sha[:8],
            "tag_url": f"{self.api_url}/{tag}",
            "commit_url": f"{self.api_url}/commits/{sha}",
        }
        return data

    def origin_tag_info(self, tag: str) -> dict:
        if not tag.startswith("refs/tags"):
            tag = f"refs/tags/{tag}"

        tag_url: str = f"{self.api_url}/{tag}"

        response: requests.Response = requests.get(tag_url, timeout=10)
        if response.status_code != 200:
            raise TagNotFoundError(tag)

        payload: dict = response.json()

        sha: str = payload.get('object', {}).get("sha", "")
        ref: str = payload.get("ref", "")

        data: dict = {
            "tag": tag.split("/")[-1],
            "ref": ref,
            "sha": sha,
            "sha8": sha[:8],
            "tag_url": tag_url,
            "commit_url": f"{self.api_url}/commits/{sha}",
        }
        return data

    def tag_info(self, *, tag: str = None, source: Literal['origin', 'workdir'] = 'workdir') -> dict:
        if source == 'workdir':
            if tag is None:
                return self.head
            return self._local_tag_info(tag)

        return self.origin_tag_info(tag)

    """

    sha => closest tag:
        git describe --exact-match d075ca43060d72ba7b1562bbae68bdc7c71185dd      (  annotated tags)
        git describe --tags  d075ca43060d72ba7b1562bbae68bdc7c71185dd            (unannotated tags)
    tag
    """

    @staticmethod
    def load(filename: str) -> dict | None:
        if not os.path.isfile(filename):
            return None
        return read_yaml(filename)

    @staticmethod
    def store(data: dict, filename: str) -> None:
        write_yaml(data, filename)


class TagNotFoundError(Exception): ...
