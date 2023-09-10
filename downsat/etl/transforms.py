from __future__ import annotations

from typing import Any, Callable, Dict, Generic, Sequence, Tuple, Union
from io import BytesIO
from zipfile import ZipFile

from attrs import field, frozen
from attrs.validators import gt

from downsat.etl import abc, types
from downsat.etl.metadata import getmeta, setmeta
from downsat.etl.utils.data_manipulation import flatten
from downsat.etl.weakref import Dict as MetaDict
from downsat.etl.weakref import List as MetaList


@frozen
class Filter(
    abc.PipelineTransform[Sequence[types.ValueType], Sequence[types.ValueType]], Generic[types.ValueType]
):
    """Filter elements of an iterable.

    Note: this is a pipeline filter, i.e. it does filter content of individual data elements in the pipeline tuple,
    but leaves the size of the tuple intact.

    # TODO: implement Filter also as a reduce operation that could reduce number of data elements in the pipeline tuple
    """

    filter_fun: Callable[[types.ValueType, Dict[str, Any]], bool]

    def __call__(self, input: Sequence[types.ValueType]) -> list[types.ValueType]:
        metadata = getmeta(input)
        result = MetaList([v for v in input if self.filter_fun(v, metadata)])  # TODO: map?
        setmeta(result, **metadata)

        return result


class UnzipBuffer(abc.PipelineTransform[BytesIO, Dict[str, BytesIO]]):
    """Transform that unzips single file from a zip archive to a file-like object or memory buffer."""

    @staticmethod
    def _unzip_buffer(
        input: BytesIO,
    ) -> dict[
        str, BytesIO
    ]:  # TODO: input: BinaryIO, but currently clash with CachedETLDatasource definition and BytesIO output of EumdacCollection
        """Unzip single file-like object into a dict of memory buffers, where keys aare filenames and corresponding values are the extracted files.

        :param input: File-like input object to be unzipped.
        :return: A dictionary where keys are filenames and values are BytesIO buffers of extracted files.
        :raises zipfile.BadZipFile: This is not a zip file.
        """

        # Create a dictionary to hold the results
        files: MetaDict[str, BytesIO] = MetaDict()

        # Open the BytesIO object as a zipfile
        with ZipFile(input, mode="r") as zf:

            # Loop through each file in the zip file
            for name in zf.namelist():

                # Open each file as BytesIO and store in the dictionary
                with zf.open(name) as f:
                    files[name] = BytesIO(f.read())
                files[name].seek(0)

        setmeta(files, **getmeta(input))

        return files

    def __call__(
        self, input: BytesIO
    ) -> Dict[
        str, BytesIO
    ]:  # TODO: input: BinaryIO, but currently clash with CachedETLDataSource definition and BytesIO output of EumdacCollection
        """Unzip single file-like object into a dict of memory buffers, where keys aare filenames and corresponding values are the extracted files.

        :param input: File-like input object to be unzipped.
        :return: A dictionary where keys are filenames and values are BytesIO buffers of extracted files.
        :raises zipfile.BadZipFile: This is not a zip file.
        """
        return self._unzip_buffer(input)


@frozen(slots=False)
# TODO: recursive definition of the input type to express that each element can be a nested tuple of InputType
class Flatten(
    abc.PipelineTransform[
        Union[types.InputType, Tuple[types.InputType, ...]],
        Union[types.InputType, Tuple[types.InputType, ...]],
    ],
    Generic[types.InputType],
):
    """Transform that flattens output of a datasource to a single tuple.

    E.g. (item1, (item2, item3)) -> (item1, item2, item3)

    :param _datasource: Datasource to be flattened.
    :param depth: Repeat flattening this many times. Must be positive.
    """

    # TODO: write tests
    # TODO: write a decorator that will simply wrap the `flatten` function and make it a transform

    depth: int = field(default=1, converter=int, validator=gt(0))

    def __call__(
        # TODO: recursive definition of the input type to express that each element can be a nested tuple of InputType
        self,
        items: types.InputType | tuple[types.InputType, ...],
    ) -> types.InputType | tuple[types.InputType, ...]:

        return flatten(items, depth=self.depth)
