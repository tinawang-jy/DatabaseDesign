import json
import os
import shutil

class DatabaseManager:
    def __init__(self, command_type=None, database_name=None, database_type=None):
        self.command_type = command_type
        self.database_name = database_name
        self.database_type = database_type
        self.root = "root"
        if not os.path.exists(self.root):
            os.makedirs(self.root)

    def create_database(self):
        if self.command_type == "create_database":
            database_dir = f"{self.root}/{self.database_name}_{self.database_type}"
            if os.path.exists(database_dir):
                print(f'Database {self.database_name} already exists.')
            else:
                os.makedirs(database_dir)
                print(f'Database {self.database_name} has been created.')

    def show_database(self):
        if self.command_type == "show_database":
            print(self.monitor())

    def monitor(self):
        metadata = []

        # Scan all db in the directory
        for db_name in os.listdir(self.root):
            db_path = os.path.join(self.root, db_name)
            if os.path.isdir(db_path):
                database_info = {"database_name": db_name,
                                 "database_path": db_path,
                                 "tables": []
                }


                # Scan all tables within the table directory
                for table_name in os.listdir(db_path):
                    table_path = os.path.join(db_path,table_name)
                    if os.path.isdir(table_path):
                        table_info = {
                            "table_name": table_name,
                            "table_path": table_path,
                            "attributes": set(),
                            "num of chunks": 0
                        }
                        for file_name in os.listdir(table_path):
                            file_path = os.path.join(table_path,file_name)
                            table_info['num of chunks'] += 1
                            if file_name.lower().endswith('.json'):
                                with open(file_path, 'r', encoding='utf-8') as file:
                                    try:
                                        data = json.load(file)
                                        if isinstance(data, list) and data:
                                            attributes = set(data[0].keys())
                                            table_info['attributes'] = table_info['attributes'] | attributes
                                    except json.JSONDecodeError as e:
                                        print(f"An error occurred while decoding JSON from file {file_name}: {e}")
                            elif file_path.lower().endswith('.csv'):
                                with open(file_path, 'r', encoding='utf-8') as file:
                                    reader = csv.reader(file)
                                    headers = next(reader, None)
                                    if headers:
                                        table_info['attributes'] = table_info['attributes'] | set(headers)
                        database_info['tables'].append(table_info)
                metadata.append(database_info)

        return metadata

    def use_database(self):
        if self.command_type == "use_database":
            records = self.monitor()
            db_names = []
            dbpath = ""
            for record in records:
                db_names.append(record['database_name'])
            if f'{self.database_name}_{self.database_type}' not in db_names:
                print(f'Database {self.database_name} not exist.')
            else:
                for record in records:
                    if record['database_name'] == f'{self.database_name}_{self.database_type}':
                        dbpath = record['database_path']
                print(f'Using {self.database_name}.')
                return dbpath

    def drop_database(self):
        if self.command_type == "drop_database":
            records = self.monitor()
            db_names = []
            for record in records:
                db_names.append(record['database_name'])
            if f'{self.database_name}_{self.database_type}' not in db_names:
                print(f'Database {self.database_name} not exist.')
            else:
                path_to_delete = f'{self.root}/{self.database_name}_{self.database_type}'
                shutil.rmtree(path_to_delete)
                print(f'Database {self.database_name} is dropped.')


if __name__ == "__main__":
    test = DatabaseManager("drop_database","xyz","relational")
    test.drop_database()
