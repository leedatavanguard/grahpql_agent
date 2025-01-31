"""Statistical exploratory data analysis utilities."""
import numpy as np
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple


class StatisticalEDA:
    """Statistical analysis utilities for exploratory data analysis."""
    
    @staticmethod
    def reduce_mem_usage(df: pd.DataFrame) -> pd.DataFrame:
        """Reduce memory usage of a DataFrame by optimizing data types.
        
        Credit: https://www.kaggle.com/gemartin/load-data-reduce-memory-usage
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with optimized memory usage
        """
        start_mem = df.memory_usage().sum() / 1024**2
        print('Memory usage of dataframe is {:.2f} MB'.format(start_mem))
        
        for col in df.columns:
            col_type = df[col].dtype
            
            if col_type != object:
                c_min = df[col].min()
                c_max = df[col].max()
                if str(col_type)[:3] == 'int':
                    if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                        df[col] = df[col].astype(np.int8)
                    elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                        df[col] = df[col].astype(np.int16)
                    elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                        df[col] = df[col].astype(np.int32)
                    elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                        df[col] = df[col].astype(np.int64)  
                else:
                    if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                        df[col] = df[col].astype(np.float32)
                    elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                        df[col] = df[col].astype(np.float32)
                    else:
                        df[col] = df[col].astype(np.float64)
            else:
                df[col] = df[col].astype('category')

        end_mem = df.memory_usage().sum() / 1024**2
        print('Memory usage after optimization is: {:.2f} MB'.format(end_mem))
        print('Decreased by {:.1f}%'.format(100 * (start_mem - end_mem) / start_mem))
        
        return df
    
    @staticmethod
    def flatten_aggregated_dataframe(
        grouped_df: pd.DataFrame,
        concat_name: bool = True,
        concat_separator: str = ' ',
        name_level: int = 1,
        inplace: bool = False
    ) -> pd.DataFrame:
        """Flatten aggregated DataFrame.

        Args:
            grouped_df: DataFrame obtained through aggregation
            concat_name: Whether to concatenate original column name and aggregation function name
            concat_separator: String to place between column name and aggregation function name
            name_level: Which element of column tuple to use for MultiIndex columns
            inplace: Whether to modify DataFrame directly
            
        Returns:
            Flattened DataFrame
        """
        if not inplace:
            grouped_df = grouped_df.copy()
            
        if isinstance(grouped_df.columns, pd.core.index.MultiIndex):
            if concat_name:
                columns = [concat_separator.join(col) for col in grouped_df.columns]
            else:
                columns = [col[name_level % 2] for col in grouped_df.columns]
            grouped_df.columns = columns
        
        return grouped_df.reset_index()

    @staticmethod
    def identify_feature_types(df: pd.DataFrame) -> Tuple[List[str], List[str]]:
        """Identify continuous and categorical features using pandas type inference.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Tuple of (continuous_features, categorical_features) lists
        """
        continuous_features = []
        categorical_features = []
        
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                continuous_features.append(col)
            else:
                categorical_features.append(col)
                
        return continuous_features, categorical_features
    
    @staticmethod
    def generate_continuous_summary(df: pd.DataFrame, continuous_features: List[str]) -> pd.DataFrame:
        """Generate summary statistics for continuous features.
        
        Args:
            df: Input DataFrame
            continuous_features: List of continuous feature names
            
        Returns:
            DataFrame with continuous feature statistics
        """
        summary_cols = [
            'Feature', 'missing_values', 'missing_percentage', 'cardinality',
            'minimum', '25th Percentile', 'mean', 'median', '75th Percentile', 'maximum', 'IQR', 'stand_dev'
        ]
        
        stats_list = []
        for feature in continuous_features:
            populated_values = df[feature].count()
            missing_values = df[feature].isnull().sum()
            missing_percentage = 100 if populated_values == 0 else ((missing_values/df[feature].size) * 100)
            
            cardinality = len(df[feature].unique())
            minimum = df[feature].min(skipna=True)
            percentile_25 = df[feature].quantile(.25)
            mean = df[feature].mean(skipna=True)
            median = df[feature].median(skipna=True)
            percentile_75 = df[feature].quantile(.75)
            maximum = df[feature].max()
            iqr = percentile_75 - percentile_25
            stand_dev = df[feature].std(skipna=True)
            
            stats_list.append([
                feature, missing_values, missing_percentage, cardinality,
                minimum, percentile_25, mean, median, percentile_75, maximum, iqr, stand_dev
            ])
            
        return pd.DataFrame.from_records(stats_list, columns=summary_cols)
    
    @staticmethod
    def generate_categorical_summary(df: pd.DataFrame, categorical_features: List[str]) -> pd.DataFrame:
        """Generate summary statistics for categorical features.
        
        Args:
            df: Input DataFrame
            categorical_features: List of categorical feature names
            
        Returns:
            DataFrame with categorical feature statistics
        """
        summary_cols = [
            'Feature', 'missing_values', 'missing_percentage', 'cardinality', 'first_mode',
            'first_mode_count', 'first_mode_percentage', 'second_mode', 'second_mode_count', 'second_mode_percentage'
        ]
        
        stats_list = []
        for feature in categorical_features:
            populated_values = df[feature].count()
            missing_values = df[feature].isnull().sum()
            missing_percentage = 100 if populated_values == 0 else ((missing_values/df[feature].size) * 100)
            
            cardinality = len(df[feature].unique())
            first_mode = 'NONE'
            first_mode_count = 'NONE'
            first_mode_percentage = 'NONE'
            second_mode = 'NONE'
            second_mode_count = 'NONE'
            second_mode_percentage = 'NONE'
            
            mode_df = df[feature].value_counts()
            
            if mode_df.size > 0:
                first_mode = str(mode_df.index[0])
                first_mode_count = int(mode_df.iloc[0])
                first_mode_percentage = ((first_mode_count / populated_values) * 100)
                
            if mode_df.size > 1:
                second_mode = str(mode_df.index[1])
                second_mode_count = int(mode_df.iloc[1])
                second_mode_percentage = ((second_mode_count / populated_values) * 100)
                
            stats_list.append([
                feature, missing_values, missing_percentage, cardinality,
                first_mode, first_mode_count, first_mode_percentage,
                second_mode, second_mode_count, second_mode_percentage
            ])
            
        return pd.DataFrame.from_records(stats_list, columns=summary_cols)
