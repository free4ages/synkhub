import hashlib
import pandas as pd
from typing import List


class HashCalculator:
    """Calculate hashes for data blocks and rows"""
    
    @staticmethod
    async def calculate_row_hash(row: pd.Series, columns: List[str]) -> str:
        """Calculate hash for a single row"""
        values = [str(row[col]) for col in columns if col in row]
        combined = "|".join(values)
        return hashlib.md5(combined.encode()).hexdigest()
    
    @staticmethod
    async def calculate_block_hash(data: pd.DataFrame, columns: List[str]) -> str:
        """Calculate hash for a block of data"""
        if columns:
            data_sorted = data.sort_values(by=columns)
        else:
            data_sorted = data
        
        all_values = []
        for _, row in data_sorted.iterrows():
            values = [str(row[col]) for col in data_sorted.columns]
            all_values.extend(values)
        
        combined = "|".join(all_values)
        return hashlib.md5(combined.encode()).hexdigest()
    
    @staticmethod
    async def calculate_data_hashes(data: pd.DataFrame, unique_keys: List[str]) -> pd.DataFrame:
        """Calculate row hashes for entire dataframe"""
        def calc_hash(row):
            values = [str(row[col]) for col in unique_keys if col in row]
            combined = "|".join(values)
            return hashlib.md5(combined.encode()).hexdigest()
        
        data = data.copy()
        data['row_hash'] = data.apply(calc_hash, axis=1)
        return data