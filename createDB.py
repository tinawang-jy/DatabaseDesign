import json
import os
import re

class DatabaseManager:
    def __init__(self, directory):
        self.directory = directory
        if not os.path.exists(directory):
            os.makedirs(directory)
        self.relational_path = os.path.join(directory, 'Relational')
        self.nosql_path = os.path.join(directory, 'NoSQL')
        if not os.path.exists(self.relational_path):
            os.makedirs(self.relational_path)
        if not os.path.exists(self.nosql_path):
            os.makedirs(self.nosql_path)
        self.databases = self._get_existing_databases()

    def _get_existing_databases(self):
        # This private method retrieves the existing databases in the directory
        existing_databases = []
        for db_type in ['Relational', 'NoSQL']:
            type_path = os.path.join(self.directory, db_type)
            for root, dirs, files in os.walk(type_path):
                for dir in dirs:
                    existing_databases.append(f"{db_type}/{dir}")
        return existing_databases

    def create_database(self, name, db_type='Relational'):
        # This method creates a new database directory
        if db_type not in ['Relational', 'NoSQL']:
            raise ValueError("Unsupported database type. Please choose 'Relational' or 'NoSQL'.")
        db_path = os.path.join(self.directory, db_type, name)
        if os.path.exists(db_path):
            raise FileExistsError(f"A database with the name '{name}' already exists.")
        os.makedirs(db_path)
        self.databases.append(f"{db_type}/{name}")
        return f"{db_type}/{name}"

    def database_monitor(self, db_name):
        # This method monitors a specific database and creates metadata in a txt file
        db_path = None
        for db in self.databases:
            if db_name in db:
                db_path = os.path.join(self.directory, db)
                break
        if not db_path:
            raise FileNotFoundError(f"No database named '{db_name}' found.")

        dir_info = []
        for root, dirs, files in os.walk(db_path):
            for d in dirs:
                subdir_info = {
                    'directory_name': d,
                    'num_files': 0,
                    'attributes': set()
                }
                subdir_path = os.path.join(root, d)
                for subroot, subdirs, subfiles in os.walk(subdir_path):
                    for file in subfiles:
                        subdir_info['num_files'] += 1
                        attributes = re.findall(r'\w+', file)
                        subdir_info['attributes'].update(attributes)
                dir_info.append(subdir_info)

        metadata = {
            'database_name': db_name,
            'tables': dir_info
        }
        metadata_path = os.path.join(db_path, f"{db_name}_metadata.txt")
        with open(metadata_path, 'w') as file:
            json.dump(metadata, file, indent=4)

        return metadata_path

    def show_existing_databases(self):
        # This method shows all existing databases
        return self.databases

# Example usage
if __name__ == "__main__":
    db_manager = DatabaseManager(".")
    print("Existing databases:", db_manager.show_existing_databases())
    created_db = db_manager.create_database("test_db", 'Relational')
    print(f"Database '{created_db}' created.")
    metadata_path = db_manager.database_monitor("test_db")
    print("Metadata file created at:", metadata_path)
    print("Updated database list:", db_manager.show_existing_databases())
