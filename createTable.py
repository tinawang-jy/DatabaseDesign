import csv
import json
import os
from createDB import DatabaseManager

class TableManager:
    def __init__(self, command_type=None, table_name=None,
                columns=None, primary_key=None, foreign_key=None, input_path=None):
        self.command_type = command_type
        self.table_name = table_name
        self.columns = columns
        self.primary_key = primary_key
        self.foreign_key = foreign_key
        self.input_path = input_path
        self.db_info = DatabaseManager().monitor()



    def show_table(self):
        if self.command_type == "show_table":
            for db in self.db_info:
                if db['database_path'] == db_path:
                    print(db['tables'])

    def new_table_nosql(self):
        data = {column: None for column in self.columns}
        os.makedirs(f'{db_path}/{self.table_name}')
        # create empty file
        with open(f'{db_path}/{self.table_name}/chunk_1.json', 'w', encoding='utf-8') as f:
            json.dump([data], f, indent=4)
            # create table metadata file
        with open(f'{db_path}/{self.table_name}/metadata.json', 'w', encoding='utf-8') as f:
            obj = {
                    "table_name": self.table_name,
                    "columns": self.columns,
                    "primary key": self.primary_key,
                    "foreign key": self.foreign_key
                                    }
            json.dump(obj, f, indent=4)
        print(f"Table {self.table_name} created in JSON.")

    def new_table_relational(self):
        os.makedirs(f'{db_path}/{self.table_name}')
        # create empty file
        with open(f'{db_path}/{self.table_name}/chunk_1.csv', 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.columns)
            writer.writeheader()  # Write the headers to the CSV file
            # create table metadata file
        with open(f'{db_path}/{self.table_name}/metadata.json', 'w', encoding='utf-8') as f:
            obj = {
                    "table_name": self.table_name,
                    "columns": self.columns,
                    "primary key": self.primary_key,
                    "foreign key": self.foreign_key
                                    }
            json.dump(obj, f, indent=4)
        print(f"Table {self.table_name} created in csv.")


    def create_table(self):
        # Input: table; columns; primary key; foreign key{self_column, ref_table, ref_column}
        # Output:  “table successfully created” or print error message
        table_info = []
        if self.command_type == "create_table":
            for db in self.db_info:
                if db['database_path'] == db_path:
                    if db_path.endswith('_nosql'):
                        # empty db
                        if len(db['tables']) == 0:
                            # check primary key
                            if self.primary_key is None:
                                print("Primary key needed!")
                            else:
                                # check foreign key
                                if self.foreign_key is not None:
                                    print(f'You are creating the first table in database. No table existing to refer.')
                                else:
                                    self.new_table_nosql()

                        # db not empty
                        else:
                            for table in db['tables']:
                                table_metadata_path = f'{table["table_path"]}/metadata.json'
                                with open(table_metadata_path, 'r', encoding='utf-8') as f:
                                    table_info.append(json.load(f))
                            # check primary key
                            if self.primary_key is None:
                                print("Please designate a primary key for your table.")
                            else:
                                # case when no foreign key
                                if self.foreign_key is None:
                                    self.new_table_nosql()
                                # case when exists foreign key
                                else:
                                    for key in self.foreign_key:
                                        # foreign key must refer to primary key
                                        for info in table_info:
                                            if info['table_name'] == key['ref_table'] and info['primary key'] == key['ref_column']:
                                                self.new_table_nosql()
                                            else:
                                                print("Check your foreign key!")

                    elif db_path.endswith('_relational'):
                        # empty db
                        if len(db['tables']) == 0:
                            # check primary key
                            if self.primary_key is None:
                                print("Primary key needed.")
                            else:
                            # check foreign key
                                if self.foreign_key is not None:
                                    print(f'You are creating the first table in database. No table existing to refer.')
                                else:
                                    self.new_table_relational()

                        # db not empty
                        else:
                            for table in db['tables']:
                                table_metadata_path = f'{table["table_path"]}/metadata.json'
                                with open(table_metadata_path, 'r', encoding='utf-8') as f:
                                    table_info.append(json.load(f))
                            # check primary key
                            if self.primary_key is None:
                                print("Please designate a primary key for your table.")
                            else:
                                # case when no foreign key
                                if self.foreign_key is None:
                                    self.new_table_relational()
                                # case when exists foreign key
                                else:
                                    for key in self.foreign_key:
                                        # foreign key must refer to primary key
                                        for info in table_info:
                                            if info['table_name'] == key['ref_table'] and info['primary key'] == key['ref_column']:
                                                self.new_table_relational()
                                            else:
                                                print("Check your foreign key!")












if __name__ == "__main__":
    db_path = "root/xyz_relational"
    test = TableManager(command_type="create_table", table_name="buyer", columns=['brand','price'], primary_key='4s_store')
    test.create_table()


