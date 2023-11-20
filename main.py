from config import db_path
from parseQuery import QueryParser
from createDB import DatabaseManager
from createTable import TableManager
from operation import Operator
from Modification import Updater
import config
from config import db_path

operator_instance = Operator()

def implement(command):
    global db_path
    if command['query_type'] == "create_database":
        worker = DatabaseManager(command_type="create_database", database_name=command["database_name"], database_type=command["type_of_database"])
        worker.create_database()
        return

    elif command['query_type'] == "show_database":
        DatabaseManager(command_type="show_database").show_database()
        return

    elif command['query_type'] == "use_database":
        worker = DatabaseManager(command_type="use_database", database_name=command["database_name"], database_type=command["type_of_database"])
        db_path = worker.use_database()
        return db_path

    elif command['query_type'] == "drop_database":
        worker = DatabaseManager(command_type="drop_database", database_name=command["database_name"], database_type=command["type_of_database"])
        worker.drop_database()
        return

    # table
    elif command['query_type'] == "show_table":
        worker = TableManager(command_type="show_table")
        worker.show_table(db_path)
        return

    elif command['query_type'] == "create_table":
        worker = TableManager(command_type="create_table", table_name=command["table_name"],columns=command["columns"], primary_key=command["primary key"], foreign_key=command["foreign key"], input_path=None)
        worker.create_table(db_path)
        return

    elif command['query_type'] == "import_table":
        worker = TableManager(command_type="import_table", table_name=command["table_name"], primary_key=command["primary key"], foreign_key=command["foreign key"], input_path=command["file_path"])
        worker.import_table(db_path)
        return

    elif command['query_type'] == "drop_table":
        worker = TableManager(command_type="drop_table", table_name=command["table_name"])
        worker.drop_table(db_path)
        return

    # modification
    elif command['query_type'] == "inserting":
        worker = Updater(command_type="inserting", table_name=command["table_name"],columns=command["columns"], condition=None, target_condition=None, values=command["values"],db_path=db_path)
        worker.insert(db_path)
        return

    elif command['query_type'] == "deleting_row":
        worker = Updater(command_type="deleting_row", table_name=command["table_name"], condition=command["condition"],db_path=db_path)
        worker.delete_row(db_path)
        return

    elif command['query_type'] == "deleting_column":
        worker = Updater(command_type="deleting_column", table_name=command["table_name"], columns=command["columns"],db_path=db_path)
        worker.delete_col(db_path)
        return

    elif command['query_type'] == "updating":
        worker = Updater(command_type="updating", table_name=command["table_name"],condition=command["condition"],target_condition=command["target_condition"],db_path=db_path)
        worker.update(db_path)
        return
    
    # aggregation
    elif command['query_type'] == "projection":
        proj_result = operator_instance.projection(command["table_name"], command["columns"],db_path=db_path)
        proj_result_list = list(proj_result)
        print("Projection Result:", json.dumps(proj_result_list, indent=4))
        
    elif command['query_type'] == "filtering":
        filter_result = operator_instance.filtering(command["table_name"], command["columns"], command["condition"],db_path=db_path)
        filter_result_list = list(filter_result)
        print("Filtering Result:", json.dumps(filter_result_list, indent=4))
        
    elif command['query_type'] == "join":
        join_result = operator_instance.join(command["selected"], command["left_table"], command["left_column"], command["right_table"], command["right_column"], command["join_method"],db_path=db_path)
        join_result_list = list(join_result)
        print("Join Result:", json.dumps(join_result_list, indent=4))
        
    elif command['query_type'] == "group_agg":
        group_agg_result = operator_instance.group_agg(command["table_name"], command["agg_column"], command["agg_method"], command["group_by_column"],db_path=db_path)
        print("Aggregation & Grouping Result:", json.dumps(group_agg_result, indent=4))
        
    elif command['query_type'] == "ordering":
        order_result = operator_instance.ordering(command["table_name"], command["order_by_column"], command["order_method"],db_path=db_path)
        print("Ordering Result", json.dumps(order_result, indent=4))


if __name__ == "__main__":
    print("Welcome to our database management system! Please first choose/create your database, use database, and then try any CRUD queries here!")
    global db_path
    while True:
        initial_query = input("Enter your command here:")
        if initial_query.lower() == "exit;":
            print("Bye Bye")
            break
        initial_query_info = QueryParser().Parser(initial_query)  # a dict
        print(initial_query_info)
        implement(initial_query_info)
        if initial_query_info['query_type'] == "use_database":
            config.set_db_path(implement(initial_query_info))
            print(db_path)

        if db_path is not None:  # Check if db_path has been set
            while True:  # Start a loop that will run until the user decides to exit
                user_input = input("Enter your CRUD command here:")
                if user_input.lower() == "exit;":
                    continue_input = input("Do you want to exit? (yes/no): ").lower()
                    if continue_input == 'yes':
                        print("Bye Bye")
                        break
                query_info = QueryParser().Parser(user_input)  # a dict
                print(query_info)
                implement(query_info)

            if user_input.lower() == "exit;":
                break

