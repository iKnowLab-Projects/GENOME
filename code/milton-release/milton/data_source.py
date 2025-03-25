"""Data Access layer for the Milton UK Biobank datasets.
"""
from collections import defaultdict, OrderedDict
import re
from functools import wraps, cached_property
import numpy as np
import pandas as pd
from pathlib import Path
from pyarrow.parquet import read_table
from dask import delayed
from dask.delayed import Delayed

from .data_desc import (
    DerivedBiomarkers,
    UkbDataDict,
    load_parquet_schema,
    INDEX_COL,
    Fld
)
from .data_info import *
from .utils import (
    with_comas,
    all_numeric,
    all_categorical,
    remap_categories,
    first_non_null_column,
    extract_field_id)
from .utils import v_concat_with_categories


class DataSourceException(Exception):
    pass    


class UkbFeatureBuilder:
    """Feature construction from raw UKB data applied to raw data extracted from
    UKB. Types of transformation include pooling various binary features into an
    aggregate, binnarizing or categorisation.
    """
    def __init__(self, feature_encodings=None, ethnicity_level=0):
        self.feature_encodings = feature_encodings or {}
        self.ethnicity_level = ethnicity_level
    
    def process(self, df):
        # Expecting df column names to include instance IDs
        df = df.rename(columns=lambda c: c.split('-')[0], copy=False)\
            .sort_index(axis=1)  # important to some features like BMI
        
        # In order to get more subjects, two BMI measurements are fused together
        # precedence is taken by the one defined as Fld.BMI, but when that is
        # not available for a subject, the other one is used.
        # To this end, Fld.BMI_v2 is renamed as Fld.BMI and processed as 
        # additional instances.
        # This feature relies on Fld.BMI being sorted *before* Fld.BMI_v2
        df = df.rename(columns={Fld.BMI_v2: Fld.BMI}, copy=False)
        
        # use either automatic or manual blood pressure reading
        df = df.rename(columns={col: col.split(',')[0] 
                                for col in UkbDataDict.BLOOD_PRESSURES},
                       copy=False)
        
        # fuse 2 versions of employment information
        df = df.rename(columns={col: Fld.EMPLOYMENT for col in df.columns
                                if col.startswith(Fld.EMPLOYMENT)},
                       copy=False)
        
        if Fld.CANNABIS_FREQ in df:
            age_at_health_questionare = (df[Fld.DATE_MENTAL_HEALTH].dt.year 
                                         - df[Fld.YEAR_OF_BIRTH])
            recent_use = (age_at_health_questionare 
                          - df[Fld.AGE_LAST_TOOK_CANNABIS] < 10)
            df[Fld.CANNABIS_FREQ] = df[Fld.CANNABIS_FREQ].where(recent_use, 0)
            
        # repetitions come from multiple feature versions (UKB nomenclature)
        columns = df.columns.unique()\
            .drop(UkbDataDict.AUXILLIARY, errors='ignore')
        
        # default feature version aggregators
        agg = {col: self.proc_column_group for col in columns}

        # specific feature transformers
        agg[Fld.OPERATIVE_PROCEDURES] = self.proc_operative_procs
        agg[Fld.END_STAGE_RENAL_DISEASE] = lambda df: df.notna().astype('float')
        agg[Fld.ETHNICITY] = self.proc_ethnicity
        
        # unfortunately, pandas as of Jul 2020 does not implement
        # .groupby(level=0, axis=1).agg(a_dict)
        result = [agg[col](df[col]) for col in columns]
        return pd.concat(result, axis=1)
    
    __call__ = process
    
    def proc_column_group(self, df):
        """Processes a column group, which comprises one or more versions
        of the same UKB feature. It first picks the first non-null value
        column-wise (since typically version 0.0 has most values). Then,
        the column is converted to categorical type, if data dictionary 
        indicates it has an encoding.
        """
        if df.ndim == 2 and not (all_numeric(df) or all_categorical(df)):
            # smart phenotype evaluation needs string cols unprocessed
            return df
        # try:
        col = first_non_null_column(df)
        col_id = extract_field_id(col.name)
        encoding = self.feature_encodings.get(int(col_id))

        if encoding is not None:
            col = remap_categories(col, encoding)
        return col    
        # except Exception as ex:
        #     raise ValueError(f'Error in in feature "{df.columns[0]}"') from ex
        
    def proc_operative_procs(self, df):
        return df.notna().sum(axis=1).astype('float')\
            .rename(Fld.OPERATIVE_PROCEDURES, copy=False)
    
    def proc_ethnicity(self, df):
        """Categorises the ethnic background feature according to the
        hierarchical data coding 1001
        (http://biobank.ctsu.ox.ac.uk/showcase/coding.cgi?id=1001).
        This coding is defined separately from the standard encoding table.
        """
        col = first_non_null_column(df)
        values = col.astype('float').to_numpy()
        encoding = self.feature_encodings[int(col.name)]
        num_categories = [float(v) for v in encoding.keys()]
        cat_names = list(encoding.values())
        
        if self.ethnicity_level == 0:
            # aggregate level-1 categorisation by replacing the 
            # detailed categories with their high-level parents
            values = np.where(values < 1000, values, values // 1000)
            
        cat_values = pd.Categorical(values, num_categories)\
            .rename_categories(cat_names)
        
        return pd.Series(cat_values, index=col.index, name=col.name)
    
    
class DfBag:
    """Bag of data frames - a simple utility that provides a map() interface 
    over a collection of pandas objects located at dask nodes.
    """
    def __init__(self, sequence):
        self.seq = sequence
        
    def map(self, func, *args, **kwargs):
        @wraps(func)
        def the_func(x):
            return func(x, *args, **kwargs)
            
        return DfBag([delayed(the_func)(x) for x in self.seq])
    
    @staticmethod
    def partitionwise(func, bags, *args, **kwargs):
        """Zips a sequence of bags creating a bag of partition sequences and
        maps a function to each such a sequence. The sequences can be lists or
        tuples. All bags are expected to be identically distributed so that
        the function can be applied to each sequence localy, on a single worker.
        """
        new_seq = map(bags.__class__,
                      zip(*(bag.seq for bag in bags)))
        return DfBag(new_seq).map(func, *args, **kwargs)
    
    def persist(self):
        """Persists the bag in memory on dask workers.
        """
        def persist(v):
            if not isinstance(v, Delayed):
                v = delayed(v)
            return v.persist()
        
        return DfBag([persist(x) for x in self.seq])
    
    def concat(self, *args, **kwargs):
        """Assuming the chunks are pandas DataFrames, gather them all up and 
        concatenate.
        """
        return delayed(v_concat_with_categories)(self.seq, *args, **kwargs)
    
    def gather(self, agg_func, *args, **kwargs):
        """Generic way of gathering the chunks by means of an 
        aggregation function.
        """
        @wraps(agg_func)
        def the_func(seq):
            return agg_func(seq, *args, **kwargs)
        return DfBag([delayed(the_func)(self.seq)])
    
    def compute(self, **kwargs):
        """For 1-element bags only, invoke .compute() on that element, otherwise
        raise an error. This is a shortcut method for a final result of a 
        sequence of transformations that have been progressively aggregated 
        with .gather()
        """
        if len(self.seq) != 1:
            raise ValueError(f'Expected 1-element bag, got {len(self.seq)}.')
        return self.seq[0].compute(**kwargs)
    
    def __iter__(self):
        return iter(self.seq)
    
    def __len__(self):
        return len(self.iter)
    
    
class ParquetDataSet:
    # This is a minimal recognized type list present in current data
    FLOAT_TYPE = 'float'
    INT_TYPE = 'int'
    STRING_TYPE = 'string'
    TS_TYPE = 'timestamp'
    
    TYPE_MAP = {
        'double': FLOAT_TYPE, 
        'int64': INT_TYPE, 
        'string': STRING_TYPE, 
        'dictionary<values=string, indices=int32, ordered=0>': STRING_TYPE,
        'timestamp[us]': TS_TYPE,
        'timestamp[ms]': TS_TYPE,
        'timestamp[ns]': TS_TYPE,
        'date32[day]': TS_TYPE, 
    }
    
    def __init__(self, path, index_col=None, cached=False, excl_ids=None, 
                 tag=None):
        """ParquetDataSet exposes Dask bag-like interface to parquet
        files as well as it implemets incremental caching.
        
        Parameters
        ----------
        path : str
          Path to the folder with parquet files or a single file
        index_col : str
          Column name to become the resulting DataFrame's index
        schema : , optional
          Parquet schema. If not provided, it's read from disk
        cached : bool
          If true, parquet files are loaded and cached in memory
        excl_ids : list of int
          List of index values to drop if present in the data set
        tag : str
          Optional string to distinguish datasets 
        """
        self.path = Path(path).absolute()
        self.index_col = index_col
        self.cached = cached
        self.excl_ids = excl_ids
        self.tag = tag
        self.files = self._find_files(self.path)
        self.schema = self._load_schema()
        self._cache = None
        self._cache_cols = {}
        
    def _load_schema(self):
        schema = {field.name: self.TYPE_MAP[str(field.type)]
                for field in load_parquet_schema(self.files[0])}
        # preprocess column names
        preprocessed_schema = {}
        for col, dtype in schema.items():

            # remove the version number
            if '-' in col:
                new_col = col.split('-', 1)[0]
            else:
                new_col = col

            if '.' in new_col:
                new_col = new_col.split('.', 1)[0]

            # leave only the subject ID
            if new_col.startswith('X') or new_col.startswith('x'):
                new_col = new_col[1:]

            preprocessed_schema[new_col] = dtype

        return preprocessed_schema
        # return schema

    @cached_property
    def index(self):
        """Unique index spanning all partitions of this dataset.
        If no index column was provided in the constructor, ValueError 
        is rised.
        """
        if not self.index_col:
            raise ValueError('Index column is not defined')
        df = self.load([]).concat().compute()
        return df.index.drop_duplicates().sort_values()
        
    def __contains__(self, column):
        return column in self.schema
    
    def __iter__(self):
        return iter(self.schema)
    
    def __getitem__(self, column):
        return self.schema[column]
        
    def _find_files(self, path):
        if path.is_dir():
            return [f for f in path.iterdir() if f.name.endswith('.parquet')]
        else:
            return [path]
        
    def num_partitions(self):
        """Returns the number of partitions this dataset comprises of.
        """
        return len(self.files)
        
    def load(self, columns, rows=None, colmap=None):
        """Returns a DfBag instance whose elements are pandas DataFrames created
        by loading and and transforming parquet files 

        Parameters
        ----------
        columns : a sequence of column names to read.
        rows : a pandas index of rows to read.
        colmap : a mapping defining renaming of column names.
        """
        columns = set(columns)
        # unknown_cols = columns.difference(self.schema)
        unknown_cols = [column for column in set(columns) if str(column) not in self.schema.keys()]
        if unknown_cols:
            # print(f'경고: 인식할 수 없는 컬럼: {sorted(unknown_cols)} (데이터셋: {self.path.name})')
            columns = columns - set(unknown_cols)
        
        # # 빈 컬럼 세트 확인
        # if not columns:
        #     print(f'오류: 모든 요청된 컬럼이 스키마에 없습니다. 원본 요청: {sorted(columns.union(unknown_cols))}')
        #     # 빈 데이터프레임 대신 최소한의 인덱스 컬럼만 로드
        #     if self.index_col and self.index_col in self.schema:
        #         print(f'인덱스 컬럼({self.index_col})만 로드합니다.')
        #         columns = {self.index_col}
        #     else:
        #         # 대안으로 스키마의 첫 번째 컬럼 사용
        #         first_col = next(iter(self.schema), None)
        #         if first_col:
        #             print(f'스키마의 첫 번째 컬럼({first_col})을 로드합니다.')
        #             columns = {first_col}
        #         else:
        #             # 정말 아무것도 없는 경우 - 빈 DataFrame 반환
        #             empty_df = pd.DataFrame()
        #             if self.index_col:
        #                 empty_df.index.name = self.index_col
        #             return DfBag([delayed(lambda: empty_df)()])
        
        def process_raw_data(df):
            # Due to the multiple field instances in UKB, it's 
            # critical to have the columns sorted before they
            # are renamed to their textual names (the colmap). 
            # Otherwise, dask and pandas will swap the columns 
            # across partitions and final results will be partly 
            # non-deterministic
            df = df.sort_index(axis=1)
            
            if colmap is not None:
                df = df.rename(columns=colmap, copy=False)
            if rows is not None:
                df = df[df.index.isin(rows)]
            return df
        
        if self.cached:
            raw_data = self._load_cached(columns)
        else:
            raw_data = DfBag(self.files)\
                .map(self._read_parquet, 
                     columns, 
                     self.index_col,
                     self.excl_ids)
        return raw_data.map(process_raw_data)
    
    def _read_raw_file(self, file_path, columns):
        def with_datetime64ns(s: pd.Series):
            """Uses common type for all datetime dtypes that might be read.
            """
            if pd.api.types.is_datetime64_any_dtype(s.dtype):
                return s.astype('datetime64[ns]')
            else:
                return s
        # categorical columns greatly speed up string matching
        # we allow for regex patterns as category column specs
        # to reflect multiple feature versions in UKB data    
        # return read_table(file_path, columns=sorted(columns))\
        #     .to_pandas(
        #         integer_object_nulls=True, 
        #         date_as_object=False, 
        #         timestamp_as_object=False, 
        #         strings_to_categorical=True,
        #         ignore_metadata=True,
        #         self_destruct=True)\
        #     .apply(with_datetime64ns)

        table = read_table(file_path, columns=sorted(columns))
        pf = table.to_pandas(
            integer_object_nulls=True, 
            date_as_object=False, 
            timestamp_as_object=False, 
            strings_to_categorical=True,
            ignore_metadata=True,
            self_destruct=True)
        return pf.apply(with_datetime64ns)
    
    def _read_parquet(self, file_path, columns, index_col, excl_ids):
        cols = {index_col} if index_col else set()
        cols = cols.union(columns)
        cols = {str(item) for item in cols}

        ##########################################
        # Insert code for debugging

        # 파일 내의 실제 컬럼 형식(X31.0.0)과 전처리된 컬럼 ID(31) 간의 매핑 생성
        # 먼저 파일 스키마를 읽어 실제 컬럼 목록 확인
        table_schema = load_parquet_schema(file_path)
        file_columns = [field.name for field in table_schema]
        
        # 요청된 컬럼 ID(예: '31')에 해당하는 실제 파일 컬럼(예: 'X31.0.0')을 찾기
        columns_to_read = []
        for file_col in file_columns:
            processed_col = file_col
            
            # 'X' 또는 'x'로 시작하면 제거
            if processed_col.startswith('X') or processed_col.startswith('x'):
                processed_col = processed_col[1:]
                
            # '.'으로 분리
            if '.' in processed_col:
                processed_col = processed_col.split('.', 1)[0]
                
            # '-'로 분리
            if '-' in processed_col:
                processed_col = processed_col.split('-', 1)[0]
                
            # 전처리된 ID가 요청된 컬럼 목록에 있으면 원본 컬럼을 읽기 목록에 추가
            if processed_col in cols:
                columns_to_read.append(file_col)
        
        # 인덱스 컬럼이 있다면 반드시 포함
        if index_col and any(c.endswith(index_col) for c in file_columns):
            index_cols = [c for c in file_columns if c.endswith(index_col)]
            columns_to_read.extend([c for c in index_cols if c not in columns_to_read])

        ##########################################

        # df = self._read_raw_file(file_path, cols)
        df = self._read_raw_file(file_path, columns_to_read)
            
        if index_col is not None:
            df.set_index(index_col, inplace=True)
            if excl_ids is not None:
                df = df.drop(excl_ids, errors='ignore') 

        return df
    
    def _load_cached(self, columns):
        new_cols = columns.difference(self._cache_cols)
        if new_cols or not columns:  # allow columns == [] to fetch the index
            self._cache_cols = new_cols.union(self._cache_cols)
            if self._cache is not None:
                # load the missing columns and concatenate
                # with currently cached data frame
                new_data = DfBag(self.files)\
                    .map(self._read_parquet, 
                         new_cols, 
                         self.index_col,
                         self.excl_ids)
                
                self._cache = DfBag.partitionwise(
                    pd.concat, 
                    [self._cache, new_data], axis=1)\
                    .persist()
            else:
                self._cache = DfBag(self.files)\
                    .map(self._read_parquet, 
                         self._cache_cols, 
                         self.index_col,
                         self.excl_ids)\
                    .persist()

        # columns 집합에 있는 필드 ID와 일치하는 실제 컬럼들을 찾기
        def select_matching_columns(df):
            # 실제 데이터프레임 컬럼으로부터 요청된 필드 ID에 해당하는 컬럼들 선택
            available_columns = []
            for col in df.columns:
                # 컬럼 이름에서 필드 ID 추출 (예: "X31.0.0" -> "31")
                field_id = None
                if col.startswith('X') or col.startswith('x'):
                    base_col = col[1:]  # 'X' 제거
                else:
                    base_col = col
                    
                # '-' 또는 '.' 기준으로 분리하여 필드 ID 추출
                if '-' in base_col:
                    field_id = base_col.split('-')[0]
                elif '.' in base_col:
                    field_id = base_col.split('.')[0]
                else:
                    field_id = base_col
                    
                # 숫자로 변환 가능하고 요청된 컬럼 목록에 있으면 선택
                try:
                    if int(field_id) in columns:
                        available_columns.append(col)
                except ValueError:
                    continue
                    
            return df[sorted(available_columns)] if available_columns else pd.DataFrame(index=df.index)
        
        return self._cache.map(select_matching_columns)

        # return self._cache.map(lambda df: df[sorted(columns)])


def _load_opt_outs(location, fname):

    return pd.read_csv(location / fname, header=None)[0]\
        .pipe(pd.Index)\
        .drop_duplicates()\
        .sort_values()
            
    
class UkbDataset:
    """Functionality for extraction of rows and columns from the UK Biobank.
    """
    def __init__(self, 
                 *,
                 location=None,
                 data_dict=None,
                 dataset=None,
                 ukb_dir=UKB_DIR,
                 drop_na_frac=1.0,
                 ethnicity_level=0,
                 opt_outs=UKB_OPT_OUT_SUBJECTS,
                 cached=True):
        """New UKB data set instance.
        
        Parameters
        ----------
        location : optional path to UKB parquet files (parent directory)
        data_dict : optional UKB data dictionary instance
        dataset : optional parquet dataset instance, when not provided a new
          one gets created.
        drop_na_frac : minimum fraction of NA values in a row to discard it 
        ethnicity_level : ethnicity is a 2-level categorical encoding. Level 0
            is the top-most and results in coarse racial categorisation.
        opt_outs : name of the file with subject IDs to exclude (always drop).
        """
        self.location = Path(location or UKB_DATA_LOCATION)
        self.parquet_path = self.location / ukb_dir
        self.drop_na_frac = drop_na_frac
        self.data_dict = data_dict or UkbDataDict(self.location, ukb_dir)
        if opt_outs:
            self.opt_outs = _load_opt_outs(self.location, opt_outs) 
        else:
            self.opt_outs = None
        
        self.feature_builder = UkbFeatureBuilder(
            self.data_dict.feature_encodings,
            ethnicity_level=ethnicity_level)
        
        if dataset:
           self.dataset = dataset
           self.cached = dataset.cached
        else: 
            self.cached = cached
            self.dataset = ParquetDataSet(
                self.parquet_path, 
                index_col=INDEX_COL,
                cached=cached,
                excl_ids=self.opt_outs,
                tag=UKB)
        
    def _get_derived(self, derived):
        if isinstance(derived, DerivedBiomarkers):
            return derived
        elif isinstance(derived, (bool, list)):
            return DerivedBiomarkers(self.location, derived)
        else:
            raise ValueError('Unsupported value of derived: %s' % derived)
        
    def to_schema(self, field_ids):
        """A shortcut method that produces a schema suitable for loading data
        from a list of string/int field IDs.
        """
        return self.data_dict.to_schema(self.data_dict[field_ids])
    
    def read_data(self, schema, rows=None, rename_cols=False):
        """Returns a Dask bag with pandas DataFrames from the UKB parquet files. 
        
        Parameters
        ----------
        schema : Series of UKB column codes and textual column names as index.
            After loading the raw UKB column codes are renamed to their textual
            versions. Note, that if derived biomarkers were requested in the
            constructor, additional columns needed for their computation will
            be added to the result.
        rows : (optional) sequence of required row identifiers. If pd.Series is
            passed, its index is used, otherwise it must be a sequence of index
            values.
        rename_cols : when True, column names are stripped of their field 
            instance information.
        
        Returns a Dask bag of DataFrames.
        """
        assert schema is not None
        
        def as_index(obj):
            if isinstance(obj, pd.Series):
                return obj.index
            elif not isinstance(obj, pd.Index):
                return pd.Index(obj)
            else:
                return obj
        
        def ensure_no_duplicates(seq):
            if not isinstance(seq, pd.Series) or not isinstance(seq, pd.Index):
                seq = pd.Index(seq)
            if seq.duplicated().any():
                raise ValueError('Duplicates detected in rows or columns.')
        
        # send column and row names to workers as the objects may be large
        ix = None
        colmap = None
        ensure_no_duplicates(schema)
        
        if rename_cols:
            # the result will strip column names of their instance versions
            # leaving just field IDs (so there will be duplicates)
            colmap = {col: col.split('-')[0] for col in schema}
            # colmap = {col: col.split('.')[0] for col in schema}
        
        if rows is not None:
            ix = as_index(rows)
            ensure_no_duplicates(ix)
        
        return self.dataset.load(columns=schema, rows=ix, colmap=colmap)
        
    def _process_data(self, df_bag, schema, derived):
        """Processes loaded UKB data according to the configured logic. Data is
        represented as a Dask bag of DataFrames. Processing involves feature
            fusion (of multiple UKB feature instances) and addition of derived
            biomarkers.
        
        Parameters
        ----------
        df_bag : Dask bag of DataFrames (eg., as returned by .read_data())
        schema : schema passed originally to read_processed()
        derived : Optional derived biomarkers object
        """
        df_bag = df_bag.map(self.feature_builder.process)
        # fields = schema.str.split('-', expand=True)[0].drop_duplicates()
        # field_names = {col: user_name for user_name, col in fields.items()}
        field_names = {col: user_name for user_name, col in schema.items()}
        # field_names = {}
        # for user_name, col in fields.items():
        #     if pd.notna(col):  # Only add mapping for non-NaN fields
        #         field_names[col] = user_name
        # print(f"field_names: {field_names}")
        pass
        
        def add_derived_and_reformat(df):
            # select only the requested columns (drop any extras required by the
            # deived biomarkers) and rename the columns to what ever names were
            # in the schema's index
            # out_df = df[field_names.to_list()]\
            #     .rename(columns=field_names, copy=False)
            # print(f"dataframe: \n{df}")
            valid_columns = [col for col in field_names.keys() if col in df.columns]
            # print(f"valid_columns: {valid_columns}")
            if not valid_columns:
                # print("No valid columns found")
                return df
            out_df = df[valid_columns].rename(columns=field_names, copy=False)
            if derived:
                out_df = pd.concat([out_df, derived.calculate(df)], axis=1)
            
            out_df = out_df.astype('category')
            return out_df
                
        return df_bag.map(add_derived_and_reformat)
            
    def read_processed(self, schema, rows=None, derived=None, sync=True, **kwargs):
        """Reads and processes UKB parquet data. Check .read_data() and
        .process_data() for details.
        
        Parameters
        ----------
        schema : selection of columns to read (see .read_data() for details)
        rows : selection of rows to read (values of the index column)
        derived : If None, don't add derived features. If list of strings, add 
            biomarkers by their names. If True, add all biomarkers.
        kwargs : other arguments passed to .process_data()
        
        Returns
        -------
        A DataFrame when sync==True, Dask bag otherwise.
        """
        if derived:
            # find out which additional fields have to be read for the 
            # derived biomarkers
            derived = self._get_derived(derived) 
            fields = [int(d) for d in derived.dependencies]
            derived_schema = self.data_dict.to_schema(
                self.data_dict.find(field_id=fields), 
                names=False)
            ext_schema = pd.concat([schema, derived_schema])\
                .drop_duplicates(keep='first')
        else:
            ext_schema = schema
        df_bag = self.read_data(ext_schema, rows=rows)
        
        # 데이터 확인
        # try:
        if hasattr(df_bag, 'seq') and df_bag.seq:
            sample = df_bag.seq[0].compute()
            if sample.empty:
                raise Exception("경고: 로드된 데이터가 비어 있습니다!")
        # except Exception as e:
        #     raise Exception(f"데이터 확인 중 오류 발생: {e}")
        
        df_bag = self._process_data(df_bag, schema, derived, **kwargs)
        
        if sync:
            # try:
            result = df_bag.concat().compute()
            return result
            # except Exception as e:
            #     raise Exception(f"concat 오류: {e}")
        
        return df_bag
        
    def read_index(self):
        """Returns pd.Index with all of the UKB subject IDs. Respects consent
        withdrawal data if defined.
        """
        return self.dataset.index
    
    
class Table:
    # Wide tables are expected to have some columns names in format:
    # <field-name>-<instance>.<measurement>
    FIELD_NAME = re.compile(r'^([0-9a-zA-Z]+)-\d+.\d+$')
    
    def __init__(self, name: str, orient: Orient, dataset: ParquetDataSet, 
                 datesof=None):
        self.name = name
        self.orient = orient
        self.dataset = dataset
        self.field_map = defaultdict(list)
        self.datesof = datesof or {}
        if orient == Orient.WIDE:
            for col in dataset:
                matches = self.FIELD_NAME.findall(col)
                field_name = matches[0] if matches else col
                self.field_map[field_name].append(col)
        else:
            for col in dataset:
                self.field_map[col].append(col)
        
    def get_schema(self, fields, expected_types=None):
        """For a sequence of field names, return their corresponding column
        names and types. For all tall datasts, column names are the same as the 
        fields, but wide datasets (UKB) may have multiple field instances.
        
        This method also raises DataSourceException when field names are not
        recognized.
        
        Returns
        -------
        Ordered dict with column names as keys and their types as values. The
        column types are those provided by ParquetDataset.
        """
        all_fields = self.field_map
        unknown = [f for f in fields if f not in all_fields]
        if unknown:
            raise DataSourceException(
                f'Unrecognized fields for table {self.name}: '
                f'{with_comas(unknown)}')
            
        if self.orient == Orient.WIDE:
            columns = sum((sorted(self.field_map[f]) for f in fields), [])
        else:
            columns = fields
        schema = OrderedDict((col, self.dataset[col]) for col in columns)
        
        if expected_types:
            bad_cols = {c for c in columns 
                        if self.dataset[c] not in expected_types}
            if bad_cols:
                if self.orient == Orient.TALL:
                    bad_fields = sorted(bad_cols)
                else:
                    # find the fields which comprise columns with wrong types
                    bad_fields = [f for f in fields 
                                  if bad_cols.intersection(self.field_map[f])]
                raise DataSourceException(
                    f'Expecting types {with_comas(expected_types)} '
                    f'for fields: {with_comas(bad_fields)}')
        return schema
    
    def datesof_schema(self, src_fields):
        """For a given list of source fields, looks up their date counterparts
        and returns the corresponding schema. 
        """
        date_fields = []
        for field in src_fields:
            f = self.datesof.get(field)
            if not f:
                raise DataSourceException(
                    f'There is no date mapping for field {field} in dataset '
                    f'{self.name}.')
            date_fields.append(f)
        return self.get_schema(date_fields, [ParquetDataSet.TS_TYPE])
        
    def load(self, columns, rows=None):
        return self.dataset.load(columns, rows=rows)
        
    def __repr__(self):
        return f'Table[{self.name}]'
        
    def __str__(self):
        return self.name
    
    def __contains__(self, field):
        return field in self.field_map
    
    def num_partitions(self):
        """Returns the number of partitions this Table consists of.
        """
        return self.dataset.num_partitions()

    
class UkbDataStore:
    """Collection of tables. Contains a basic functionality to load a set of
    tables and access their data/attributes in a uniform manner.
    """
    ORIENT = {
        UKB: Orient.WIDE,
        OLINK: Orient.WIDE
    }
    
    # mappings from some of a dataset's fields representing events
    # to fields representing event dates
    DATESOF_MAP = {
        UKB: {
            '41270': '41280',  # Diagnoses - ICD10
            '41271': '41281',  # Diagnoses - ICD9
            '41273': '41283',  # Operative procedures - OPCS3
            '41274': '41282',  # Operative procedures - OPCS4
            '20002': '20008',  # Non-cancer illness code, self-reported
            '20001': '20006',  # Cancer code, self-reported
            '20004': '20010',  # SR operations
            '40001': '40000',  # primary cause of death
            '40002': '40000',  # secondary cause of death
            '40006': '40005',  # Cancer diagnosis
        },
        GP_SCRIPTS: {
            'bnf_code': 'issue_date',
            'dmd_code': 'issue_date',
            'drug_name': 'issue_date',
        },
        GP_CLINICAL: {
            'read_2': 'event_dt',
            'read_3': 'event_dt',
        },
        HESIN_OPER: {
            'oper3': 'opdate',
            'oper4': 'opdate',
        },
    }
    
    def __init__(self, *, location=None, cached=True, names=None, 
                 opt_outs=UKB_OPT_OUT_SUBJECTS):
        self.cached = cached
        self.location = Path(location or UKB_DATA_LOCATION)
        self.opt_outs = opt_outs
        if opt_outs:
            self.opt_outs = _load_opt_outs(self.location, opt_outs)
        # collection of references to parquet resources
        datasets = self._init_datasets(names or DATASETS)
        self.tables = self._make_tables(datasets)
        
    def _make_tables(self, datasets):
        tables = {}
        for name, dataset in datasets.items():
            orient = self.ORIENT.get(name, Orient.TALL)
            datesof = self.DATESOF_MAP.get(name)
            tables[name] = Table(name, orient, dataset, datesof) 
        return tables
        
    def _find_most_recent(self, prefix):
        paths = list(self.location.iterdir())
        found = sorted(p for p in paths if re.match(prefix, p.name))
        if not found:
            raise ValueError('Cannot find any files with following name '
                                f'pattern: {prefix}')
        return found[-1]
    
    def _most_recent_dataset(self, prefix, tag):
        return ParquetDataSet(self._find_most_recent(prefix), 
                              cached=self.cached,
                              index_col=INDEX_COL,
                              excl_ids=self.opt_outs,
                              tag=tag)
            
    def _init_datasets(self, names):
        return {
            # use the same name pattern for all non-UKB datasets
            name:  self._most_recent_dataset(fr'^{name}.*\.parquet$', tag=name)
            for name in names 
        }
    
    def __getitem__(self, name):
        return self.tables[name]
    
    def __contains__(self, name):
        return name in self.tables
    
    def __iter__(self):
        return iter(self.tables)
    
    def column_names(self, table, fields):
        """Return a conglomerate of dataset's column names that jointly comprise
        the given field names. This is mostly used for the UKB dataset since
        it features multiple instances of individual fields.
        """
        return self.tables[table].column_names(fields)

    @cached_property
    def ukb(self):
        return UkbDataset(dataset=self.tables[UKB].dataset)
