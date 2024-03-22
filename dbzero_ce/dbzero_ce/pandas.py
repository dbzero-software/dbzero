from pandas.core.arrays.base import (
    ExtensionArray,
)
from pandas.core.api import StringDtype, Int64Dtype
from pandas._typing import (
        Any,
        PositionalIndexer,
        ArrayLike
    )

from pandas import DataFrame

from pandas.core.internals import (
    ArrayManager,
)
from pandas.core.indexes.api import Index
import numpy as np
import itertools

import dbzero_ce as db0


class BlockInterface(ExtensionArray):
    """ needed:
    * _from_sequence
    * _from_factorized
    * __getitem__
    * __len__
    * __eq__
    * dtype
    * nbytes
    * isna
    * take
    * copy
    * _concat_same_type
    * interpolate"""
    def __init__(self, values):
        self._nbytes = 1
        self._values =  db0.block()
        for value in values:
            self._values.append(value)
        self._dtype = BlockInterface.storage_class_to_dtype(self._values.get_storage_class())

    @property
    def dtype(self):
        return self._dtype
    
    @property
    def nbytes(self):
        return self._nbytes
    
    @staticmethod
    def storage_class_to_dtype(storage_class):
        if storage_class == 2:
            return StringDtype()
        elif storage_class == 4:
            return Int64Dtype()

    @classmethod
    def _from_sequence(cls, scalars, *, dtype = None, copy: bool = False):
        """
        Construct a new ExtensionArray from a sequence of scalars.

        Parameters
        ----------
        scalars : Sequence
            Each element will be an instance of the scalar type for this
            array, ``cls.dtype.type`` or be converted into this type in this method.
        dtype : dtype, optional
            Construct for this particular dtype. This should be a Dtype
            compatible with the ExtensionArray.
        copy : bool, default False
            If True, copy the underlying data.

        Returns
        -------

        """
        return cls([str(scalars) for scalar in scalars])

    @classmethod
    def _from_sequence_of_strings(
        cls, strings, *, dtype = None, copy: bool = False
    ):
        """
        Construct a new ExtensionArray from a sequence of strings.

        Parameters
        ----------
        strings : Sequence
            Each element will be an instance of the scalar type for this
            array, ``cls.dtype.type``.
        dtype : dtype, optional
            Construct for this particular dtype. This should be a Dtype
            compatible with the ExtensionArray.
        copy : bool, default False
            If True, copy the underlying data.

        Returns
        -------
        ExtensionArray

        Examples
        --------
        >>> pd.arrays.IntegerArray._from_sequence_of_strings(["1", "2", "3"])
        <IntegerArray>
        [1, 2, 3]
        Length: 3, dtype: Int64
        """
        return cls(strings) 
    
    def __getitem__(self, item: PositionalIndexer):
        if isinstance(item, slice):
            indeces = [i for i in range(len(self._values))][item]
            values = []
            for i in indeces:
                values.append(self._values[i])
            return BlockInterface(values)
        else:
            return self._values[item]
    
    def __setitem__(self, key, newvalue):
        if isinstance(key, int):
            self._values[key] = newvalue
        else:
            for k in key:
                self._values[k] = newvalue

    def __len__(self) -> int:
        """
        Length of this array

        Returns
        -------
        length : int
        """
        return len(self._values)
     
    def __lt__(self, other) -> ArrayLike:
        vals = []
        for i in range(len(self._values)):
            vals.append(self._values[i])
        if not isinstance(other, list):
            other = [other]
        return np.array([a < b for (a, b) in itertools.product(vals, other)])

    def __eq__(self, other: Any) -> ArrayLike:  # type: ignore[override]
        """
        Return for `self == other` (element-wise equality).
        """
        # Implementer note: this should return a boolean numpy ndarray or
        # a boolean ExtensionArray.
        # When `other` is one of Series, Index, or DataFrame, this method should
        # return NotImplemented (to ensure that those objects are responsible for
        # first unpacking the arrays, and then dispatch the operation to the
        # underlying arrays)
        vals = []
        for i in range(len(self._values)):
            vals.append(self._values[i])
        if not isinstance(other, list):
            other = [other]
        return np.array([a == b for (a, b) in itertools.product(vals, other)])
    
    def isna(self):
        """
        A 1-D array indicating if each value is missing.

        Returns
        -------
        numpy.ndarray or pandas.api.extensions.ExtensionArray
            In most cases, this should return a NumPy ndarray. For
            exceptional cases like ``SparseArray``, where returning
            an ndarray would be expensive, an ExtensionArray may be
            returned.

        Notes
        -----
        If returning an ExtensionArray, then

        * ``na_values._is_boolean`` should be True
        * `na_values` should implement :func:`ExtensionArray._reduce`
        * ``na_values.any`` and ``na_values.all`` should be implemented

        Examples
        --------
        >>> arr = pd.array([1, 2, np.nan, np.nan])
        >>> arr.isna()
        array([False, False,  True,  True])
        """
        raise np.array([x is None for x in self._values])
    
    def take(
        self,
        indices,
        *,
        allow_fill: bool = False,
        fill_value: Any = None,
    ):
        """
        Take elements from an array.

        Parameters
        ----------
        indices : sequence of int or one-dimensional np.ndarray of int
            Indices to be taken.
        allow_fill : bool, default False
            How to handle negative values in `indices`.

            * False: negative values in `indices` indicate positional indices
              from the right (the default). This is similar to
              :func:`numpy.take`.

            * True: negative values in `indices` indicate
              missing values. These values are set to `fill_value`. Any other
              other negative values raise a ``ValueError``.

        fill_value : any, optional
            Fill value to use for NA-indices when `allow_fill` is True.
            This may be ``None``, in which case the default NA value for
            the type, ``self.dtype.na_value``, is used.

            For many ExtensionArrays, there will be two representations of
            `fill_value`: a user-facing "boxed" scalar, and a low-level
            physical NA value. `fill_value` should be the user-facing version,
            and the implementation should handle translating that to the
            physical version for processing the take if necessary.

        Returns
        -------
        ExtensionArray

        Raises
        ------
        IndexError
            When the indices are out of bounds for the array.
        ValueError
            When `indices` contains negative values other than ``-1``
            and `allow_fill` is True.

        See Also
        --------
        numpy.take : Take elements from an array along an axis.
        api.extensions.take : Take elements from an array.

        Notes
        -----
        ExtensionArray.take is called by ``Series.__getitem__``, ``.loc``,
        ``iloc``, when `indices` is a sequence of values. Additionally,
        it's called by :meth:`Series.reindex`, or any other method
        that causes realignment, with a `fill_value`.

        Examples
        --------
        Here's an example implementation, which relies on casting the
        extension array to object dtype. This uses the helper method
        :func:`pandas.api.extensions.take`.

        .. code-block:: python

           def take(self, indices, allow_fill=False, fill_value=None):
               from pandas.core.algorithms import take

               # If the ExtensionArray is backed by an ndarray, then
               # just pass that here instead of coercing to object.
               data = self.astype(object)

               if allow_fill and fill_value is None:
                   fill_value = self.dtype.na_value

               # fill value should always be translated from the scalar
               # type for the array, to the physical storage type for
               # the data, before passing to take.

               result = take(data, indices, fill_value=fill_value,
                             allow_fill=allow_fill)
               return self._from_sequence(result, dtype=self.dtype)
        """
        # Implementer note: The `fill_value` parameter should be a user-facing
        # value, an instance of self.dtype.type. When passed `fill_value=None`,
        # the default of `self.dtype.na_value` should be used.
        # This may differ from the physical storage type your ExtensionArray
        # uses. In this case, your implementation is responsible for casting
        # the user-facing type to the storage type, before using
        # pandas.api.extensions.take


        #  need to implement slice interface in dbzero 
        values = []
        for i in indices:
            values.append(self._values[i])
        return BlockInterface(values)
    
    def copy(self):
        """
        Return a copy of the array.

        Returns
        -------
        ExtensionArray

        Examples
        --------
        >>> arr = pd.array([1, 2, 3])
        >>> arr2 = arr.copy()
        >>> arr[0] = 2
        >>> arr2
        <IntegerArray>
        [1, 2, 3]
        Length: 3, dtype: Int64
        """
        vals = []
        for value in self._values:
            vals.append(value)
        return BlockInterface(vals)
    
    @classmethod
    def _concat_same_type(cls, to_concat):
        """
        Concatenate multiple array of this dtype.

        Parameters
        ----------
        to_concat : sequence of this type

        Returns
        -------
        ExtensionArray

        Examples
        --------
        >>> arr1 = pd.array([1, 2, 3])
        >>> arr2 = pd.array([4, 5, 6])
        >>> pd.arrays.IntegerArray._concat_same_type([arr1, arr2])
        <IntegerArray>
        [1, 2, 3, 4, 5, 6]
        Length: 6, dtype: Int64
        """
        # Implementer note: this method will only be called with a sequence of
        # ExtensionArrays of this class and with the same dtype as self. This
        # should allow "easy" concatenation (no upcasting needed), and result
        # in a new ExtensionArray of the same dtype.
        # Note: this strict behaviour is only guaranteed starting with pandas 1.1
        result = []
        for elem in to_concat:
            result += elem._values

        return BlockInterface(result)
    
    def interpolate(
        self,
        *,
        method,
        axis: int,
        index: Index,
        limit,
        limit_direction,
        limit_area,
        fill_value,
        copy: bool,
        **kwargs,
    ):
        """
        See DataFrame.interpolate.__doc__.

        Examples
        --------
        >>> arr = pd.arrays.NumpyExtensionArray(np.array([0, 1, np.nan, 3]))
        >>> arr.interpolate(method="linear",
        ...                 limit=3,
        ...                 limit_direction="forward",
        ...                 index=pd.Index([1, 2, 3, 4]),
        ...                 fill_value=1,
        ...                 copy=False,
        ...                 axis=0,
        ...                 limit_area="inside"
        ...                 )
        <NumpyExtensionArray>
        [0.0, 1.0, 2.0, 3.0]
        Length: 4, dtype: float64
        """
        # NB: we return type(self) even if copy=False
        raise NotImplementedError(
            f"{type(self).__name__} does not implement interpolate"
        )

class DB0DataFrame(DataFrame):
    def __init__(self, data):
        if not isinstance(data, dict):
            "Dataframe implemented only for dicts"
        blocks = [BlockInterface(column) for column in data.values()]
        indexes = [
            [x for x in range(len(list(data.values())[0]))],
            list(data.keys())
        ]
        frame = db0.dataframe()
        object.__setattr__(self, "_dataframe", frame)
        for block in blocks:
            self._dataframe.append_block(block._values)
        for index in indexes:
            db0list = db0.list()
            for value in index:
                db0list.append(value)
            self._dataframe.append_index(db0list) 
        mgr = ArrayManager(arrays = blocks, axes=indexes)
        super().__init__(data=mgr)