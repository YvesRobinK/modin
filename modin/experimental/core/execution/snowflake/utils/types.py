import pandas
from snowflake.snowpark.types import LongType, StringType
import numpy


def _map_to_dtypes(
        sf_type
):
    if isinstance(sf_type, LongType):
        return numpy.dtypes.Int64DType
    elif isinstance(sf_type, StringType):
        return numpy.dtypes.ObjectDType



class DtypeWrapper():

    def __init__(
            self,
            sf_table
    ):
        self._sf_types = []
        for col in sf_table.schema:
            self._sf_types.append(col.datatype)
        dtypes = [_map_to_dtypes(x) for x in self._sf_types]
        self.values = pandas.Series(dtypes).values