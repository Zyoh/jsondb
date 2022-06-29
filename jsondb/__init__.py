from typing import Optional, Hashable, Any, TextIO, Sequence
from pathlib import Path
import json
import io


class Jsondb:
    def __init__(self, path: Path | str, caching: Optional[bool] = True):
        self.__path: Path = Path(path)
        # Index caching
        self.caching = caching
        self.__index_cache: dict | None = None

    @property
    def path(self) -> Path:
        return self.__path

    def add(self, new_items: dict, /) -> None:
        """
        Add new entries.
        Parameters
        ----------
        new_items: dict
            New entries to add.
        """
        self.path.touch(exist_ok=True)
        with open(self.path, "r+") as f:
            index = self._get_index(f, truncate=True)
            self.__index_cache = None

            # Move to end of file and add newline. If file is empty, do not add newline.
            f.seek(0, io.SEEK_END)
            f.seek(max(f.tell() - 1, 0))

            # Add items to db.
            for key, value in new_items.items():
                # Add new position to index.
                index.update({
                    key: index.get(key, []) + [f.tell()]
                })

                # Add item to db.
                f.write(json.dumps({key: value}) + "\n")

            # Add index back to end of file.
            index_pos = f.tell()
            f.write(json.dumps(index) + '\n')
            f.write(str(index_pos))

    def _get_index(self, file: TextIO, *, truncate: Optional[bool] = False) -> dict:
        """
        Returns the database index and, optionally, removes it from the database.
        Parameters
        ----------
        file: TextIO
            Database file.
        truncate: Optional[bool]
            If True, remove the index information from the database.

        Returns
        -------
        dict
            Database index.
        """
        if self.__index_cache is not None:
            return self.__index_cache

        # Return to original pos after.
        original_pos: int = file.tell()

        index: dict = {}

        # Get last line pos. Contains index pos as str.
        file.seek(0, io.SEEK_END)
        pos: int = file.tell()
        while pos > 1:
            file.seek(pos)
            if file.read(1) == '\n':
                break
            pos -= 1

        if pos < 1:
            return index
        file.seek(pos)

        # Get index.
        try:
            # Get line for index pos data
            line: str = file.read()
            index_pos: int = int(line.strip())
            # Seek to start of index
            file.seek(index_pos)
            # Read up to start of index pos data
            line: str = file.read(pos - index_pos)
            index: dict = json.loads(line)

            if truncate:
                # Delete last line.
                file.seek(index_pos)
                file.truncate()
        except (ValueError, json.decoder.JSONDecodeError):
            pass

        file.seek(original_pos)
        if self.caching:
            self.__index_cache = index
        return index

    def get_many(self, keys: Sequence[Hashable], *, silent: Optional[bool] = False) -> dict[Hashable, list]:
        """
        Gets every value found for the given keys.
        Parameters
        ----------
        keys: Sequence[Hashable]
            Return any values matching these keys.
        silent: Optional[bool]
            Silences error if file doesn't exist.
        Returns
        -------
        dict[Hashable, list]
            A dictionary of keys, each with a list of matched values.
        """
        res: dict[Hashable, list] = {}

        if not self.path.exists():
            if silent:
                return res
            else:
                raise FileNotFoundError(f"No file found at {str(self.path)}")

        with open(self.path, "r") as f:
            index = self._get_index(f)

            for key in keys:
                if target_positions := index.get(key):
                    res[key] = []
                    for target_pos in target_positions:
                        f.seek(target_pos)
                        res[key].extend(list(i for i in json.loads(f.readline()).values()))

        return res

    def get(self, key: Hashable, *, silent: Optional[bool] = False) -> list[Any]:
        """
        Gets every value found for a given key.
        Parameters
        ----------
        key: Hashable
            Return any values matching this keys.
        silent: Optional[bool]
            Silences error if file doesn't exist.
        Returns
        -------
        list[Any]
            A list of matched values.
        """
        res: dict[Hashable, list] = self.get_many([key], silent=silent)
        return res.get(key, [])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
