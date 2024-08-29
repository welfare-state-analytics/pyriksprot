from __future__ import annotations

import datetime
import os
import re
from typing import Literal

import pygit2
import requests
from more_itertools import first

from .utility import load_json, read_yaml, write_yaml

ts = datetime.datetime.fromtimestamp


def gh_get_url(*, user: str, repository: str, path: str, filename: str, tag: str) -> str:
    return f"https://raw.githubusercontent.com/{user}/{repository}/{tag}/{path}/{filename}"


def gh_repository_url(*, user: str, repository: str) -> str:
    return f"https://github.com/{user}/{repository}"


def gh_ls(user: str, repository: str, path: str = "", tag: str = "main") -> list[dict]:
    url: str = f"https://api.github.com/repos/{user}/{repository}/contents/{path}?ref={tag}"
    data: list[dict] = load_json(url)
    return data


def gh_tags(folder: str) -> list[str]:
    """Returns tags in given local git repository"""
    repo: pygit2.Repository = pygit2.Repository(path=folder)
    rx: re.Pattern = re.compile(r'^refs/tags/v\d+\.\d+\.\d+$')
    tags: list[str] = sorted([r.removeprefix('refs/tags/') for r in repo.references if rx.match(r)])
    return tags


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


class TagNotFoundError(Exception):
    ...
