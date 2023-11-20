import json
import os
import operator
import csv
from createDB import DatabaseManager
from createTable import DatabaseManager
from config import db_path
'''Insert
Input: table_name, columns=[], values=[]
Output: 
Delete_row
Input: table, condition{variable, method, value}
Output: 
delete _column
Input: table, columns
Output: 
Update
Input: table, condition{variable, method, value}, target_condition{target, value}
Output: 
'''

class Updater:
    def __init__(self, command_type=None, table_name=None,
                columns=None, condition=None, target_condition=None, values=None,db_path=None):
        self.command_type = command_type
        self.table_name = table_name
        self.columns = columns
        self.condition = condition
        self.target_condition = target_condition
        self.values = values
        self.db_path = db_path
        self.db_info = DatabaseManager().monitor()
        self.table_info = self.monitor(self.db_path) # a list of dict
        self.db_type = self.get_dbtype(self.db_path)


    def get_dbtype(self,db_path):
        _, _, database_type = db_path.rpartition('_')
        return database_type

    def monitor(self,db_path):
        table_info = []
        for db in self.db_info:
                if db['database_path'] == db_path:
                    for table in db['tables']:
                        table_metadata_path = f'{table["table_path"]}/metadata.json'
                        with open(table_metadata_path, 'r', encoding='utf-8') as f:
                            table_info.append(json.load(f))
        return table_info


    def check_value_exist(self,value_to_check,col_name,path,dbtype):
        # Verify that the path exists and is a directory
        if not os.path.exists(path) or not os.path.isdir(path):
            print(f"Error: The path {path} does not exist or is not a directory.")
            return False

        # Depending on dbtype, set the file extension to search for
        file_extension = '.json' if dbtype == 'nosql' else '.csv'

        # Iterate over the files directly in the specified directory
        for file_name in os.listdir(path):
            file_path = os.path.join(path, file_name)
            # Check if the file matches the expected file type
            if os.path.isfile(file_path) and file_path.lower().endswith(file_extension):
                with open(file_path, 'r', encoding='utf-8') as file:
                    try:
                        # If nosql, expect a JSON format
                        if dbtype == 'nosql':
                            data = json.load(file)
                            for record in data:
                                if col_name in record and record[col_name] == value_to_check:
                                    print(f"Value '{value_to_check}' found in file: {file_name}")
                                    return True
                        # If relational, expect a CSV format
                        elif dbtype == 'relational':
                            reader = csv.DictReader(file)
                            for record in reader:
                                if record[col_name] == value_to_check:
                                    print(f"Value '{value_to_check}' found in file: {file_name}")
                                    return True
                    except (json.JSONDecodeError, KeyError) as e:
                        print(f"An error occurred while processing file {file_name}: {e}")
                    except Exception as e:
                        print(f"An unexpected error occurred: {e}")

        # If the loop completes without finding the value, print a message
        print(f"Value '{value_to_check}' not found in any files under {path}.")
        return False

    def extract_number(self,filename):
        num_part = filename.split('_')[-1]  # Assuming format 'chunk_{number}.json'
        number = int(num_part.split('.')[0])  # Extract the number and convert to int
        return number

    def bubble_sort(self,arr):
        n = len(arr)
        for i in range(n):
            for j in range(0, n-i-1):
                if self.extract_number(arr[j]) > self.extract_number(arr[j+1]):
                    arr[j], arr[j+1] = arr[j+1], arr[j]
        return arr


    def write(self, columns, values, dbtype,db_path):
        path = f"{db_path}/{self.table_name}"
        if dbtype == 'nosql':
            # Find the last JSON file in the directory
            files = [f for f in os.listdir(path) if f.startswith("chunk") and f.endswith('.json')]
            sorted_files = self.bubble_sort(files)
            if sorted_files:
                last_file = os.path.join(path, files[-1])
                with open(last_file, 'r+', encoding='utf-8') as file:
                    data = json.load(file)
                    data.append(dict(zip(columns, values)))
                    file.seek(0)  # Go back to the start of the file
                    json.dump(data, file, indent=4)
                    file.truncate()  # Truncate the file to the new size
                print(f"New data written to {last_file}")
            else:
                print("No JSON files found to write to.")

        elif dbtype == 'relational':
            # Find the last CSV file in the directory
            files = [f for f in os.listdir(path) if f.startswith("chunk") and f.endswith('.csv')]
            sorted_files = self.bubble_sort(files)
            if sorted_files:
                last_file = os.path.join(path, files[-1])
                with open(last_file, 'r+', newline='', encoding='utf-8') as file:
                    reader = csv.reader(file)
                    headers = next(reader, None)
                    if not headers or set(columns) != set(headers):
                        print("Column mismatch error. New data not written.")
                        return

                    writer = csv.writer(file)
                    file.seek(0, os.SEEK_END)  # Move to the end of the file
                    writer.writerow(values)
                print(f"New data written to {last_file}")
            else:
                print("No CSV files found to write to.")



    def insert(self,db_path):
        if self.command_type == "inserting":
            # check if table exists
            tb_names = []
            for tb in self.table_info:
                tb_names.append(tb["table_name"])
            if self.table_name not in tb_names:
                print(f"Table {self.table_name} doesn't exist.")
            else:
                # check if pk is in columns
                for tb in self.table_info:
                    if tb["table_name"] == self.table_name:
                        if tb["primary key"] not in self.columns:
                            print("Primary key must be included when inserting.")
                            return
                        else:
                # check if table refers to other table
                            if not tb["foreign key"]:
                                pair = dict(zip(self.columns,self.values))
                                pk_col = tb["primary key"]
                                duplicate = self.check_value_exist(value_to_check=pair[pk_col],col_name=pk_col,path=f"{db_path}/{self.table_name}",dbtype=self.db_type)
                                # false means no duplicate
                                if duplicate is False:
                                    self.write(columns=self.columns, values=self.values, dbtype=self.db_type, db_path=db_path)
                                else:
                                    print("Primary key value duplicate.")
                            else:
                            # check if self_column value exist in referred table
                                for fk in tb["foreign key"]:
                                    referring_path = f"{db_path}/{fk['ref_table']}"
                                    pair = dict(zip(self.columns,self.values))
                                    self_col = tb["foreign key"][0]["self_column"]
                                    pk_col = tb["primary key"]
                                    fk_col = tb["foreign key"][0]["ref_column"]
                                    fk_validation = self.check_value_exist(value_to_check=pair[self_col],col_name=fk_col,path=referring_path,dbtype=self.db_type)
                                    # check duplicate pk
                                    duplicate = self.check_value_exist(value_to_check=pair[pk_col],col_name=pk_col,path=f"{db_path}/{self.table_name}",dbtype=self.db_type)
                                    if fk_validation and (duplicate is False):
                                        self.write(columns=self.columns, values=self.values, dbtype=self.db_type,db_path=db_path)
                                    else:
                                        if fk_validation == False:
                                            print("Foreign key value doesn't exist in foreign table.")
                                        if duplicate == True:
                                            print("Primary key value duplicate.")

    def del_col_from_file(self,dir_path:str,columns:[],dbtype:str):
        file_extension = '.json' if dbtype == 'nosql' else '.csv'

        for file_name in os.listdir(dir_path):
            file_path = os.path.join(dir_path, file_name)
            if os.path.isfile(file_path) and file_path.lower().endswith(file_extension):
                if dbtype == 'nosql':
                    # Process JSON files
                    with open(file_path, 'r+', encoding='utf-8') as file:
                        data = json.load(file)
                        # Remove specified columns
                        updated_data = [{k: v for k, v in item.items() if k not in columns} for item in data]
                        file.seek(0)
                        json.dump(updated_data, file, indent=4)
                        file.truncate()
                elif dbtype == 'relational':
                    # Process CSV files
                    with open(file_path, 'r', encoding='utf-8') as file:
                        reader = csv.DictReader(file)
                        updated_data = [{k: v for k, v in row.items() if k not in columns} for row in reader]

                    with open(file_path, 'w', newline='', encoding='utf-8') as file:
                        writer = csv.DictWriter(file, fieldnames=reader.fieldnames)
                        writer.writeheader()
                        writer.writerows(updated_data)
        return print(f"Columns {columns} removed from all {dbtype} files in {dir_path}")

    def delete_col(self,db_path):
        # Input: table, columns
        if self.command_type == "deleting_column":
            # check table exist
            tb_names = []
            for tb in self.table_info:
                print(tb)
                tb_names.append(tb["table_name"])
            if self.table_name not in tb_names:
                print(f"Table {self.table_name} doesn't exist.")
            else:
                # check if pk in columns
                for tb in self.table_info:
                    if tb["table_name"] == self.table_name:
                        if tb["primary key"] in self.columns:
                            print("Cannot delete Primary key column.")
                            return
                        else:
                            self.del_col_from_file(dir_path=f"{db_path}/{self.table_name}",columns=self.columns,dbtype=self.db_type)
                            #update metadata
                            with open(f"{db_path}/{self.table_name}/metadata.json", 'r+', encoding='utf-8') as f:
                                data = json.load(f)
                                new = [item for item in data["columns"] if item not in self.columns]
                                data["columns"] = new
                                f.seek(0)
                                json.dump(data, f, indent=4)
                                f.truncate()
                            print("metadata updated.")

    def check_value_exist_in_ref(self, value_to_check, ref_by, col_name,dbtype):
        for ref in ref_by:
            if self.check_value_exist(value_to_check, col_name, f"{db_path}/{ref['table_name']}", dbtype):  # Assuming dbtype is nosql for simplicity
                return True
        return False


    def filter_check_del(self, dbtype:str, ref_by:list, db_path):
        ops = {
    '>': operator.gt,
    '<': operator.lt,
    '=': operator.eq,
    '!=': operator.ne,
    '>=': operator.ge,
    '<=': operator.le
}
        path = f"{db_path}/{self.table_name}"
        pk = None
        for tb in self.table_info:
            if tb["table_name"] == self.table_name:
                pk = tb["primary key"]
                break

        if pk is None:
            print(f"No primary key found for table {self.table_name}.")
            return

        file_extension = '.json' if dbtype == 'nosql' else '.csv'
        for file_name in os.listdir(path):
            file_path = os.path.join(path, file_name)
            if os.path.isfile(file_path) and file_path.lower().endswith(file_extension) and ('metadata' not in file_path):
                if dbtype == 'nosql':
                    with open(file_path, 'r+', encoding='utf-8') as file:
                        data = json.load(file)
                        updated_data = []

                        for record in data:
                            filter = ops[self.condition['method']](record[self.condition['variable']],self.condition['value'])
                            if filter and (not ref_by or not self.check_value_exist_in_ref(record[pk], ref_by, pk,dbtype)):
                                continue  # Skip adding this record to updated_data
                            updated_data.append(record)

                        file.seek(0)
                        json.dump(updated_data, file, indent=4)
                        file.truncate()
                    print("Row deleted.")
                elif dbtype == 'relational':
                    rows_to_keep = []
                    with open(file_path, 'r', newline='', encoding='utf-8') as file:
                        reader = csv.DictReader(file)
                        for row in reader:
                            filter = ops[self.condition['method']](row[self.condition['variable']],self.condition['value'])
                            if filter and (not ref_by or not self.check_value_exist_in_ref(row[pk], ref_by, pk,dbtype)):
                                continue
                            rows_to_keep.append(row)

                    with open(file_path, 'w', newline='', encoding='utf-8') as file:
                        writer = csv.DictWriter(file, fieldnames=reader.fieldnames)
                        writer.writeheader()
                        writer.writerows(rows_to_keep)
                    print("Row deleted")

        # if dbtype == "nosql", all files in json format; if dbtype=='relational', all files in csv format
        # for each file listed under path, read each line
        # if this line meets the condition in self.condition, for example, price > 100
            # if ref_by is empty, delete row
            # if ref_by not empty
                # further check if the pk value in this line exist in another table using check_value_exist(self,value_to_check,col_name,path,dbtype)
                    # pk exist in ref_by table, print error
                    # else, delete row


    def delete_row(self,db_path):
        if self.command_type == "deleting_row":
            # Input: table, condition{variable, method, value}
            # DELETE FROM cars IF price > “40000”;
            # check table exist
            tb_names = []
            for tb in self.table_info:
                tb_names.append(tb["table_name"])
            if self.table_name not in tb_names:
                print(f"Table {self.table_name} doesn't exist.")
                return
            else:
                for tb in self.table_info:
                    if tb["table_name"] == self.table_name:
                        # check if condition var exist
                        if self.condition["variable"] not in tb["columns"]:
                            print(f"Column {self.condition['variable']} not exist.")
                            return
                        else:

                            referred_by = [] # a list of tb_info dict that refer to self.table_name
                            for tb in self.table_info:
                                if tb["foreign key"]:
                                    for fk in tb["foreign key"]:
                                        if fk["ref_table"] == self.table_name:
                                            referred_by.append(tb)


                            self.filter_check_del(dbtype=self.db_type, ref_by=referred_by,db_path=db_path)

    def update_col(self, dbtype:str,db_path):
        ops = {
    '>': operator.gt,
    '<': operator.lt,
    '=': operator.eq,
    '!=': operator.ne,
    '>=': operator.ge,
    '<=': operator.le
}
        path = f"{db_path}/{self.table_name}"
        pk = None
        for tb in self.table_info:
            if tb["table_name"] == self.table_name:
                pk = tb["primary key"]
                break


        file_extension = '.json' if dbtype == 'nosql' else '.csv'
        for file_name in os.listdir(path):
            file_path = os.path.join(path, file_name)
            if os.path.isfile(file_path) and file_path.lower().endswith(file_extension):
                if dbtype == 'nosql':
                    with open(file_path, 'r+', encoding='utf-8') as file:
                        data = json.load(file)
                        updated_data = []

                        for record in data:
                            if self.condition['variable'] in record:
                                filter_passes = ops[self.condition['method']](record[self.condition['variable']], self.condition['value'])

                                if filter_passes is True:
                                    record[self.target_condition['target']] = self.target_condition['value']

                            updated_data.append(record)

                        file.seek(0)
                        json.dump(updated_data, file, indent=4)
                        file.truncate()
                    print(f"Records updated in {file_path}.")
                elif dbtype == 'relational':
                    rows_to_keep = []
                    with open(file_path, 'r', newline='', encoding='utf-8') as file:
                        reader = csv.DictReader(file)
                        for row in reader:
                            # Check if the condition variable is present in the row
                            if self.condition['variable'] in row:
                                filter_passes = ops[self.condition['method']](row[self.condition['variable']], self.condition['value'])
                                # Update the record if the condition is met
                                if filter_passes:
                                    row[self.target_condition['target']] = self.target_condition['value']

                            rows_to_keep.append(row)

        # Write the updated rows back to the file
                    with open(file_path, 'w', newline='', encoding='utf-8') as file:
                        writer = csv.DictWriter(file, fieldnames=reader.fieldnames)
                        writer.writeheader()
                        writer.writerows(rows_to_keep)
                    print(f"Records updated in {file_path}.")

    def update(self,db_path):
        '''{
                "query_type": "updating",
                "table_name": match.group(1),
                "condition":{
                    "variable":condition_match.group(1),
                    "method": condition_match.group(2),
                    "value": condition_match.group(3)
                },
                "target_condition":{
                    "target": target_match.group(1),
                    "value": target_match.group(2),
                },
            }
'''
        if self.command_type == "updating":
            # Input: table, condition{variable, method, value}, target_condition{target, value}
            # check table exist
            tb_names = []
            for tb in self.table_info:
                tb_names.append(tb["table_name"])
            if self.table_name not in tb_names:
                print(f"Table {self.table_name} doesn't exist.")
            else:
                # check if pk in target
                for tb in self.table_info:
                    if tb["table_name"] == self.table_name:
                        if tb["primary key"] == self.target_condition["target"]:
                            print("Cannot update Primary key column.")
                            return
                        # check if condition var exist
                        else:
                            if self.condition["variable"] not in tb["columns"]:
                                print(f"Column {self.condition['variable']} not exist.")
                                return
                            else:
                                self.update_col(dbtype=self.db_type,db_path=db_path)








if __name__ == "__main__":
    db_path = "root/xyz_relational"
    test = Updater(command_type="updating",table_name="basic", condition={"variable":"maximum_nights","method":"=","value":'365'},target_condition={"target":"maximum_nights","value":"whole year"})


    test.update()
