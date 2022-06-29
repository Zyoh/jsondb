#  MIT License
#
#  Copyright (c) 2022 Zoe <zoe@zyoh.ca>
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.
#

from typing import Optional, Hashable, Any, TextIO, Sequence, Callable
from pathlib import Path
import json
import io

from jsondb.errors import *


class Jsondb:
    def __init__(self, path: Path | str) -> None:
        """
        Connects to a database file.
        Parameters
        ----------
        path: Path | str
            Path to the database file.
        """
        self.__path: Path = Path(path)
        self.__index: dict = {}

        self.path.touch(exist_ok=True)

        self.__fio: TextIO | None = None
        self.open()

    @property
    def path(self) -> Path:
        return self.__path

    def open(self) -> None:
        """
        Open database file for IO access.
        """
        if self.__fio is None:
            self.__fio: TextIO = open(self.path, "r+")

    def flush(self) -> None:
        """
        Manually trigger writing all changes to disk.
        """
        if self.__fio is not None:
            self.__fio.flush()

    def close(self) -> None:
        """
        Close database file.
        """
        self.__index = {}
        if self.__fio is not None:
            self.__fio.close()
            self.__fio = None

    @staticmethod
    def requires_fio(f: Callable) -> Callable:
        def wrapper(self, *args, **kwargs):
            if self.__fio is None:
                raise ClosedDatabaseError()
            return f(self, *args, **kwargs)
        return wrapper

    def _load_index(self, truncate: Optional[bool] = False, force: Optional[bool] = False) -> None:
        """
        Returns the database index and, optionally, removes it from the database.
        Parameters
        ----------
        truncate: Optional[bool]
            If True, remove the index information from the database.
        force: Optional[bool]
            If True, always (re)load index. Otherwise, only load if not already loaded.
        """

        # Return to original pos after.
        original_pos: int = self.__fio.tell()

        # Get last line pos. Contains index pos as str.
        self.__fio.seek(0, io.SEEK_END)
        index_pointer_pos: int = self.__fio.tell()
        while index_pointer_pos > 1:
            self.__fio.seek(index_pointer_pos)
            if self.__fio.read(1) == '\n':
                break
            index_pointer_pos -= 1

        if index_pointer_pos < 1:
            return

        # Get index.
        try:
            # Get line for index pos data.
            self.__fio.seek(index_pointer_pos)
            index_pos: int = int(self.__fio.read().strip())

            # Don't retrieve if cached unless forced.
            if (self.__index == {}) or force:
                # Seek to start of index.
                self.__fio.seek(index_pos)
                # Read up to start of index pos data.
                line: str = self.__fio.read(index_pointer_pos - index_pos)
                self.__index = json.loads(line)

            if truncate:
                # Delete last line.
                self.__fio.seek(index_pos)
                self.__fio.truncate()
        except (ValueError, json.decoder.JSONDecodeError):
            pass

        self.__fio.seek(original_pos)

    @requires_fio
    def add(self, new_items: dict, /) -> None:
        """
        Add new entries.
        Parameters
        ----------
        new_items: dict
            New entries to add.
        """
        self._load_index(truncate=True)

        # Move to end of file and add newline. If file is empty, do not add newline.
        self.__fio.seek(0, io.SEEK_END)
        self.__fio.seek(max(self.__fio.tell() - 1, 0))

        # Add items to db.
        for key, value in new_items.items():
            # Add new position to index.
            self.__index.update({
                key: self.__index.get(key, []) + [self.__fio.tell()]
            })

            # Add item to db.
            self.__fio.write(json.dumps({key: value}) + "\n")

        # Add index back to end of file.
        index_pos = self.__fio.tell()
        self.__fio.write(json.dumps(self.__index) + '\n')
        self.__fio.write(str(index_pos))

    @requires_fio
    def get_many(self, keys: Sequence[Hashable]) -> dict[Hashable, list[Any]]:
        """
        Gets every value found for the given keys.
        Parameters
        ----------
        keys: Sequence[Hashable]
            Return any values matching these keys.
        Returns
        -------
        dict[Hashable, list[Any]]
            A dictionary of keys, each with a list of matched values.
        """
        res: dict[Hashable, list[Any]] = {}

        self._load_index()

        for key in keys:
            if target_positions := self.__index.get(key):
                res[key] = []
                for target_pos in target_positions:
                    self.__fio.seek(target_pos)
                    res[key].extend(list(i for i in json.loads(self.__fio.readline()).values()))

        return res

    @requires_fio
    def get(self, key: Hashable) -> list[Any]:
        """
        Gets every value found for a given key.
        Parameters
        ----------
        key: Hashable
            Return any values matching this keys.
        Returns
        -------
        list[Any]
            A list of matched values.
        """
        res: dict[Hashable, list[Any]] = self.get_many([key])
        return res.get(key, [])

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
