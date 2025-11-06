import pandas as pd
import numpy as np
# from datasketch import MinHash, MinHashLSH
import sqlite3
from collections import defaultdict
import time

class DatabaseProfiler:
    def __init__(self,table_map, db_path='cloud_costs.db'):
        """
        Init the db profiler
        """
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.profile_map={}
        # self.tables=[
        #     'aws_cost_usage',
        #     'azure_cost_usage',
        # ]
        self.tables=table_map
    
    def profile_all_tables(self):
        """Returns the statistical profiling of all tables."""
        for table_name in self.tables:
            self.profile_map[table_name]=self.profile_table(table_name=table_name)
        return self.profile_map
    

    def profile_table(self, table_name):
        """Basic table profiling from an sql db"""
        print(table_name)
        print("ALL TABLES:")
        df_tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", self.conn)
        print(df_tables)
        print("="*50)        
        df = pd.read_sql(f"SELECT * FROM {table_name}", self.conn)
        
        profile = {
            'table_name': table_name,
            'row_count': int(len(df)),  # Convert to native int
            'columns': {}
        }
        
        for column in df.columns:
            col_data = df[column]

            profile['columns'][column] = {
                'null_count': int(col_data.isnull().sum()),  # Convert to native int
                'distinct_count': int(col_data.nunique()),   # Convert to native int
                'data_type': str(col_data.dtype),
                'sample_values': col_data.dropna().head(10).tolist(),
                'min_value': float(col_data.min()) if col_data.dtype in [np.int64, np.float64] else None,  # Convert to float
                'max_value': float(col_data.max()) if col_data.dtype in [np.int64, np.float64] else None,  # Convert to float
            }
            
            # String-specific profiling
            if col_data.dtype == 'object':
                str_lengths = col_data.dropna().astype(str).str.len()
                profile['columns'][column].update({
                    'min_length': int(str_lengths.min()) if len(str_lengths) > 0 else None,  # Convert to int
                    'max_length': int(str_lengths.max()) if len(str_lengths) > 0 else None,  # Convert to int
                    'common_patterns': self._extract_patterns(col_data)
                })
        
        return profile
    
    def _extract_patterns(self, series):
        """Extract common patterns from string data"""
        # Implement pattern detection (digits, uppercase, special chars, etc.)
        patterns = defaultdict(int)
        for value in series.dropna():
            if isinstance(value, str):
                # Simple pattern categorization
                if value.isdigit():
                    patterns['all_digits'] += 1
                elif value.isalpha():
                    patterns['all_letters'] += 1
                elif any(c.isdigit() for c in value) and any(c.isalpha() for c in value):
                    patterns['alphanumeric'] += 1
        return dict(patterns)
    
    # def create_minhash_index(self, table_name, column_name, num_perm=128):
    #     """Create MinHash sketches for similarity detection"""
    #     df = pd.read_sql(f"SELECT {column_name} FROM {table_name}", self.conn)
    #     values = df[column_name].dropna().astype(str)
        
    #     lsh = MinHashLSH(threshold=0.5, num_perm=num_perm)
    #     minhashes = {}
        
    #     for idx, value in enumerate(values.head(1000)):  # Sample for efficiency
    #         m = MinHash(num_perm=num_perm)
    #         for word in value.split():
    #             m.update(word.encode('utf8'))
    #         lsh.insert(f"{table_name}.{column_name}_{idx}", m)
    #         minhashes[f"{table_name}.{column_name}_{idx}"] = m
        
    #     return lsh, minhashes
    

if __name__=="__main__":
    profiler=DatabaseProfiler()
    total_profile=profiler.profile_all_tables()
    # print(total_profile)        
    for profile in total_profile:
        print(total_profile[profile])
        print("=="*50)
        # time.sleep(10)

