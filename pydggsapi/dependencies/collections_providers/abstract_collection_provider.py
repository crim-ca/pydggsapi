from pydggsapi.schemas.api.collections import ZoneDataPropertyQuantizationMethod
from pydggsapi.schemas.api.collection_providers import (
    CollectionProviderGetDataDictReturn,
    CollectionProviderGetDataReturn,
)
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Generic, Optional, TypeVar
from pygeofilter.ast import AstType
import pandas as pd
import xarray as xr
import numpy as np


@dataclass
class AbstractDatasourceInfo(ABC):
    data_cols: List[str] = field(default_factory=lambda: ["*"])
    exclude_data_cols: List[str] = field(default_factory=list)
    zone_groups: Optional[Dict[str, str]] = field(default_factory=dict)
    datetime_col: str = None
    nodata_mapping: Optional[Dict[str, Any]] = field(default_factory=lambda: {"default": np.nan})


QuantizerDataType = TypeVar('QuantizerDataType')


class AbstractQuantizer(ABC, Generic[QuantizerDataType]):
    @classmethod
    @abstractmethod
    def quantize_zones(
        cls,
        zones_data: QuantizerDataType,
        zones_mapping: Dict[str, List[str]],
        property_quantize_method: ZoneDataPropertyQuantizationMethod,
        zone_id_column: Optional[str] = None,
        datetime_column: Optional[str] = None,
    ) -> QuantizerDataType:
        """
        :param zones_data: data within an index of zoneIds and all other relevant columns that must be quantized
        :param zones_mapping: mapping of desired quantized zoneIds to expected children zoneIds composing them
        :param property_quantize_method: mapping of pandas-column/dggs-property names to quantization methods
        :param zone_id_column: name of the column representing zoneIds, defaults to 'zoneId'
        :param datetime_column: name of the column representing datetimes, defaults to 'datetime'
        :return: quantized zones dataset
        """
        raise NotImplementedError


class AbstractCollectionProvider(AbstractQuantizer, ABC):
    datasources: Dict[str, AbstractDatasourceInfo]

    # 1. The return data must be aggregated.
    # 2. The return consist of 4 parts (zoneIds, cols_name, cols_dtype, data)
    # 3. The zoneIds is the list of zoneID , its length must align with data's length
    # 4. cols_name and cols_dtype length must align
    # 5. data is the data :P
    # 6. In case of exception, return an empty CollectionProviderGetDataReturn, ie. all with []
    @abstractmethod
    def get_data(
        self,
        zoneIds: List[str],
        res: int,
        datasource_id: str,
        cql_filter: AstType | None,
        include_datetime: bool = False,
        include_properties: List[str] = None,
        exclude_properties: List[str] = None,
        quantize_zones_mapping: Dict[str, List[str]] = None,
        quantize_property_methods: ZoneDataPropertyQuantizationMethod = None,
    ) -> CollectionProviderGetDataReturn:
        raise NotImplementedError

    @abstractmethod
    def get_datadictionary(self, datasource_id: str) -> CollectionProviderGetDataDictReturn:
        raise NotImplementedError


class PandasQuantizer(AbstractQuantizer[pd.DataFrame]):
    """
    Zone data quantization strategy using ``pandas.DataFrame`` as input and output.

    To be combined with AbstractCollectionProvider implementations as applicable.
    """

    @classmethod
    def quantize_zones(
        cls,
        zones_data: pd.DataFrame,
        zones_mapping: Dict[str, List[str]],
        property_quantize_method: ZoneDataPropertyQuantizationMethod,
        zone_id_column: Optional[str] = None,
        datetime_column: Optional[str] = None,
    ) -> pd.DataFrame:
        if not zones_mapping:
            return zones_data
        zone_id_col = zone_id_column if zone_id_column else 'zoneId'
        datetime_col = datetime_column if datetime_column else 'datetime'
        data_cols = set(zones_data.columns.tolist()) - {zone_id_col, datetime_col}
        agg_map = {
            'sum': 'sum',
            'mean': 'mean',
            'max': 'max',
            'min': 'min',
            'median': 'median',
            'mode': lambda x: x.mode().iloc[0] if not x.mode().empty else x.iloc[0]
        }
        agg_funcs = {
            col: agg_map.get(str(property_quantize_method.get(col)), 'sum')
            for col in data_cols
        }
        if datetime_col in zones_data.columns:
            agg_funcs[datetime_col] = 'first'
        mapping_df = pd.DataFrame([
            {'parent': parent, 'child': child}
            for parent, children in zones_mapping.items()
            for child in children
        ])
        merged = zones_data.merge(mapping_df, left_on=zone_id_col, right_on='child')
        zones_result = merged.groupby('parent').agg(agg_funcs).reset_index().rename(columns={'parent': zone_id_col})
        return zones_result


class XarrayQuantizer(AbstractQuantizer[xr.Dataset]):
    """
    Zone data quantization strategy using ``xarray.Dataset`` as input and output.

    To be combined with AbstractCollectionProvider implementations as applicable.
    """

    @classmethod
    def quantize_zones(
        cls,
        zones_data: xr.Dataset,
        zones_mapping: Dict[str, List[str]],
        property_quantize_method: ZoneDataPropertyQuantizationMethod,
        zone_id_column: Optional[str] = None,
        datetime_column: Optional[str] = None,
    ) -> xr.Dataset:
        if not zones_mapping:
            return zones_data

        # use dataframe since xarray groupby-aggregation
        # does not support many-to-one mapping directly
        data_df = zones_data.to_dataframe().reset_index()
        data_df = PandasQuantizer.quantize_zones(
            zones_data=data_df,
            zones_mapping=zones_mapping,
            property_quantize_method=property_quantize_method,
            zone_id_column=zone_id_column,
            datetime_column=datetime_column,
        )
        zone_id_column = zone_id_column if zone_id_column else 'zoneId'
        result = data_df.set_index(zone_id_column).to_xarray()
        return result


class DatetimeNotDefinedError(ValueError):
    pass
