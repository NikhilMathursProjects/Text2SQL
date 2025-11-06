import sqlite3
import pandas as pd
import os
from pathlib import Path
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class DatabaseSetup:
    def __init__(self,required_data_map,dir_path='uploads', db_path='cloud_costs.db'):
        self.db_path = db_path
        self.data_dir = Path(dir_path)
        # self.required_data_map = {
        #     'aws_cost_usage': self.data_dir/'aws_cost_usage.csv',
        #     'azure_cost_usage': self.data_dir/'azure_cost_usage.csv'
        # }
        self.required_data_map=required_data_map
        self.conn = None
        self.loaded_tables = {}
    
    def check_data_files(self) -> Dict[str, bool]:
        """Check which required data files exist"""
        existing_files = {}
        missing_files = {}
        
        for table_name, file_path in self.required_data_map.items():
            if file_path.exists():
                existing_files[table_name] = file_path
                logger.info(f"Found {table_name}: {file_path}")
            else:
                missing_files[table_name] = file_path
                logger.error(f"Missing {table_name}: {file_path}")
        
        if missing_files:
            logger.warning(f"Missing {len(missing_files)} files. Only processing existing files.")
        
        return existing_files
    
    def create_connection(self) -> bool:
        """Create database connection"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            logger.info(f"Connected to database: {self.db_path}")
            return True
        except sqlite3.Error as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def load_data(self) -> bool:
        """Load all existing datasets into database"""
        existing_files = self.check_data_files()
        
        if not existing_files:
            logger.error("No data files found to load!")
            return False
        
        success_count = 0
        for table_name, file_path in existing_files.items():
            if self._load_single_dataset(table_name, file_path):
                success_count += 1
        
        logger.info(f"📊 Successfully loaded {success_count} out of {len(existing_files)} datasets")
        return success_count > 0
    
    def _load_single_dataset(self, table_name: str, file_path: Path) -> bool:
        """Load a single CSV file into database table"""
        try:
            logger.info(f"Loading {table_name} from {file_path.name}")
            
            #read CSV file
            df = pd.read_csv(file_path)
            logger.info(f"Read {len(df)} rows, {len(df.columns)} columns")
            
            # Clean the dataframe
            df_cleaned = self.clean_dataframe(df, file_path.name)
            
            # Load into database
            df_cleaned.to_sql(table_name, self.conn, if_exists='replace', index=False)
            
            # Store metadata
            self.loaded_tables[table_name] = {
                'original_rows': len(df),
                'cleaned_rows': len(df_cleaned),
                'columns': list(df_cleaned.columns),
                'file_source': file_path.name
            }
            
            logger.info(f"Successfully loaded {table_name} ({len(df_cleaned)} rows)")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load {table_name}: {e}")
            return False
    
    def clean_dataframe(self, df: pd.DataFrame, data_source: str) -> pd.DataFrame:
        """Clean and prepare dataframe for database storage"""
        logger.info(f"Cleaning {data_source} data")
        
        original_shape = df.shape
        original_columns = df.columns.tolist()
        
        # 1.Remove completely empty columns
        df_cleaned = df.dropna(axis=1, how='all')
        
        # 2.Remove completely empty rows (only if all values are null)
        df_cleaned = df_cleaned.dropna(how='all')
        
        # 3.Clean column names for SQL compatibility
        df_cleaned.columns = [self.clean_column_name(col) for col in df_cleaned.columns]
        
        # 4. Fill numeric nulls with 0
        numeric_cols = df_cleaned.select_dtypes(include=['number']).columns
        df_cleaned[numeric_cols] = df_cleaned[numeric_cols].fillna(0)
        
        # 5. Fill text nulls with 'Unknown'
        text_cols = df_cleaned.select_dtypes(include=['object']).columns
        df_cleaned[text_cols] = df_cleaned[text_cols].fillna('Unknown')
        
        # Log cleaning results
        removed_cols = set(original_columns) - set(df_cleaned.columns)
        if removed_cols:
            logger.info(f"Removed {len(removed_cols)} empty columns: {list(removed_cols)}")
        
        row_diff = original_shape[0] - len(df_cleaned)
        if row_diff > 0:
            logger.info(f"Removed {row_diff} completely empty rows")
        
        logger.info(f"Final shape: {df_cleaned.shape} (was {original_shape})")
        
        return df_cleaned
    
    def clean_column_name(self, column_name: str) -> str:
        """Clean column name for SQL compatibility"""
        # Convert to lowercase and replace spaces/special characters
        cleaned = str(column_name).strip().lower()
        cleaned = cleaned.replace(' ', '_').replace('-', '_').replace('/', '_')
        cleaned = ''.join(c for c in cleaned if c.isalnum() or c == '_')

        if cleaned and cleaned[0].isdigit():
            cleaned = 'col_' + cleaned
            
        return cleaned
    
    def verify_database(self) -> bool:
        """Verify that tables were created correctly"""
        try:
            cursor = self.conn.cursor()
            
            # Get all tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            print(tables)
            logger.info("Verifying database tables:")
            # logger.info("=" * 50)
            
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                row_count = cursor.fetchone()[0]
                
                cursor.execute(f"PRAGMA table_info({table})")
                columns = [col[1] for col in cursor.fetchall()]
                print(columns)
                logger.info(f"{table}:")
                logger.info(f"   Rows: {row_count:,}")
                logger.info(f"   Columns: {len(columns)}")
                logger.info(f"   Sample columns: {columns[:5]}")
                
                #Store the verified info
                if table in self.loaded_tables:
                    self.loaded_tables[table]['verified_rows'] = row_count
                    self.loaded_tables[table]['verified_columns'] = columns
            
            logger.info("Database verification complete!")
            return True
            
        except Exception as e:
            logger.error(f"Database verification failed: {e}")
            return False
    
    def run_sample_queries(self) -> bool:
        """Run sample queries to demonstrate the data is accessible"""
        try:
            print("------------LOADING SAMPLE QUERIES------------------\n")
            logger.info("Running sample queries")
            
            for table_name in self.loaded_tables.keys():
                logger.info(f"\n--- {table_name.upper()} ---")
                
                #sample rows
                sample_query = f"SELECT * FROM {table_name} LIMIT 2"
                sample_data = pd.read_sql(sample_query, self.conn)
                
                if not sample_data.empty:
                    print(sample_data.to_string(index=False))
                else:
                    logger.info("No data to display")
            
            return True
            
        except Exception as e:
            logger.error(f"Sample queries failed: {e}")
            return False
    
    def get_database_summary(self) -> Dict:
        """Get a summary of the loaded database"""
        print("-------------Getting DB Summary")
        summary = {
            'database_path': self.db_path,
            'tables_loaded': len(self.loaded_tables),
            'tables': {}
        }
        
        for table_name, info in self.loaded_tables.items():
            summary['tables'][table_name] = {
                'rows': info.get('verified_rows', 0),
                'columns': info.get('verified_columns', []),
                'source_file': info.get('file_source', 'Unknown')
            }
        print("summary complete (there is actually a summary(its not empty))")
        return summary
    
    def setup_complete(self) -> bool:
        """Complete database setup pipeline"""
        logger.info("Starting Database Setup...")

        #Check data files
        existing_files = self.check_data_files()
        if not existing_files:
            logger.error("No data files found. Please check your mock_data_sets folder.")
            return False
        
        #Create database connection
        if not self.create_connection():
            return False
        
        #Load data
        if not self.load_data():
            return False
        
        #Verify everything worked
        if not self.verify_database():
            return False
        
        # Step 5: Test with sample queries
        self.run_sample_queries()
        
        # Step 6: Print summary
        summary = self.get_database_summary()
        print(summary)

        logger.info("Database setup completed successfully!")
        return True


if __name__ == "__main__":
    setup = DatabaseSetup()
    # success = setup.setup_complete()
    setup.create_connection()
    setup.verify_database()
    # summ=setup.get_database_summary()
    # print(summ)
    # if success:
    #     print("\nLayer 1 Complete! Ready for Layer 2: Metadata Extraction")
    # else:
    #     print("\nSetup failed. Check the logs above.")