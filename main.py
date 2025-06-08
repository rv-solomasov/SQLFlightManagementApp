import json
import logging
import os
import sqlite3

from tabulate import tabulate


class DataClass:
    def __init__(self, target_table: str, attrs: dict):
        self.target_table = target_table
        self.attrs = attrs or {}

    def __setitem__(self, k, v):
        self.attrs[k] = v

    def __getitem__(self, k):
        return self.attrs[k]

    def __delitem__(self, k):
        del self.attrs[k]

    def get_columns(self):
        return list(map(getattr(str, "lower"), self.attrs.keys()))

    def get_values(self):
        return tuple(self.attrs.values())

    def __str__(self):
        return f"[{self.target_table}]: \n{json.dumps(self.attrs, indent=1)}"


def atoi(s: str):
    return int(s) if s.isdigit() else s


class DBOperations:
    def __init__(self, name: str):
        self.sql_create_base = {
            "pilots": """
                CREATE TABLE IF NOT EXISTS Pilots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    license_number TEXT UNIQUE NOT NULL,
                    flight_hours INTEGER NOT NULL CHECK (flight_hours >= 0)
                );
            """,
            "destinations": """
                CREATE TABLE IF NOT EXISTS Destinations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    city TEXT NOT NULL,
                    country TEXT NOT NULL,
                    airport_code TEXT UNIQUE NOT NULL
                );
            """,
            "flights": """
                CREATE TABLE IF NOT EXISTS Flights (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    flight_number TEXT UNIQUE NOT NULL,
                    source_id INTEGER NOT NULL,
                    destination_id INTEGER NOT NULL,
                    departure_time TEXT NOT NULL,
                    arrival_time TEXT NOT NULL,
                    status TEXT NOT NULL,
                    pilot_id INTEGER,
                    FOREIGN KEY (source_id) REFERENCES Destinations(id),
                    FOREIGN KEY (destination_id) REFERENCES Destinations(id),
                    FOREIGN KEY (pilot_id) REFERENCES Pilots(id)
                );
            """,
        }

        self.sql_insert = "INSERT INTO {table} ({columns}) VALUES ({values});"
        self.sql_search = "SELECT * FROM {table} WHERE {condition};"
        self.sql_update = "UPDATE {table} SET {field} = ? WHERE {condition};"
        self.sql_delete = "DELETE FROM {table} WHERE {condition};"
        self.sql_drop = "DROP TABLE IF EXISTS {table};"

        self.current_dir = os.path.dirname(os.path.abspath(__file__))
        self.dbname = name.capitalize() + ".db"
        logging.basicConfig(
            filename="flight_management_{name}.log".format(name=name),
            level=logging.DEBUG,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )
        if not os.path.exists(self.dbname):
            try:
                self.initialize_tables()
            except Exception as e:
                logging.error(e)

    def get_connection(self):
        return sqlite3.connect(self.dbname)

    def initialize_tables(self):
        for table in self.sql_create_base.keys():
            self.create_table(table)
            self.populate_table(table)

    def create_table(self, table_name: str):
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(self.sql_create_base[table_name.lower()])
                conn.commit()
                logging.info(
                    "Table {table} created successfully".format(table=table_name)
                )
            except KeyError:
                logging.error(
                    "Cannot find DDL for table {table}".format(table=table_name)
                )
            except Exception as e:
                logging.error(e)

    def populate_table(self, table_name: str):
        try:
            file_path = os.path.join(
                self.current_dir, "src", "{table}.csv".format(table=table_name.lower())
            )
            header = []
            with open(file_path, "r") as f:
                header = list(map(getattr(str, "lower"), f.readline().split(",")))
                for line in f.readlines():
                    fields = [field.strip() for field in line.strip().split(",")]
                    data = DataClass(
                        target_table=table_name,
                        attrs=dict(zip(header, fields)),
                    )
                    self._insert_data(data)
            logging.info("Succesfully populated table {table}".format(table=table_name))
        except Exception as e:
            logging.error(e)

    def drop_table(self, table_name: str):
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                query = self.sql_drop.format(table=table_name.capitalize())
                logging.debug(f"Running:\n{query}")
                cursor.execute(query)
                conn.commit()
                logging.info(
                    "Table {table} dropped successfully".format(table=table_name)
                )
            except Exception as e:
                logging.error(e)

    def get_table_columns(self, table_name: str):
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(f"PRAGMA table_info({table_name});")
                columns_info = cursor.fetchall()
                return [col[1] for col in columns_info]  # col[1] is the column name
            except Exception as e:
                logging.error(f"Failed to get columns for {table_name}: {e}")
                return []

    def _insert_data(self, data: DataClass):
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                query = self.sql_insert.format(
                    table=data.target_table.capitalize(),
                    columns=",".join(data.get_columns()),
                    values=",".join(["?"] * len(data.get_columns())),
                )

                logging.debug(f"Running:\n{query} with {data.get_values()}")
                cursor.execute(
                    query,
                    data.get_values(),
                )

                conn.commit()
                logging.info("Inserted row successfully")
            except Exception as e:
                logging.error(e)

    def insert_data(self, table_name: str):
        columns = self.get_table_columns(table_name)
        if not columns:
            print(f"Table {table_name} does not exist or has no columns.")
            return
        attrs = {}
        print(
            "Enter values for the following fields (leave blank to skip, `EXIT` to cancel):"
        )
        for col in columns:
            if col == "id":
                continue  # skip autoincrement id
            value = input(f"{col}: ")
            if value.strip().upper() == "EXIT":
                print("Insert cancelled.")
                return
            if value.strip() == "":
                continue
            attrs[col] = atoi(value)
        if not attrs:
            print("No data entered.")
            return
        data = DataClass(target_table=table_name, attrs=attrs)
        self._insert_data(data)
        self.search_data(
            table_name=table_name,
            id="(SELECT MAX(id) FROM {table})".format(table=table_name),
        )

    def select_all(self, table_name: str):
        return self.search_data(table_name=table_name, show_all=True)

    def show(self, table_name: str, data: list[tuple]):
        headers = self.get_table_columns(table_name)
        print(tabulate(data, headers=headers, tablefmt="grid"))

    def teardown(self):
        if input("Are you sure?(y/n): ") == "y":
            os.remove(os.path.join(self.current_dir, test.dbname))

    def search_data(self, table_name: str, show_all: bool = False, id: int = None):
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                if show_all:
                    query = self.sql_search.format(table=table_name, condition="True")
                elif id is not None:
                    query = self.sql_search.format(
                        table=table_name, condition="id = {id}".format(id=id)
                    )
                else:
                    columns = self.get_table_columns(table_name=table_name)
                    filter_by = dict(zip(range(1, len(columns) + 1), columns))
                    choose_column = int(
                        input(
                            "Choose a column to filter by:\n"
                            + "\n".join([f"{k}) {v}" for k, v in filter_by.items()])
                            + "\n"
                        )
                    )
                    filter_column = filter_by[choose_column]
                    value = atoi(input("Enter a value to filter by:\n"))

                    query = self.sql_search.format(
                        table=table_name, condition=f"{filter_column} = {value}"
                    )

                logging.debug(f"Running:\n{query}")
                cursor.execute(query)
                data = cursor.fetchall()
                return self.show(table_name, data)
            except Exception as e:
                logging.error(e)

    def update_data(self, table_name: str):
        exit, success = [False, False]
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                columns = self.get_table_columns(table_name)
                if "id" not in columns:
                    print("No 'id' column found in this table.")
                    return
                while not exit:
                    print("Enter `EXIT` to quit at any time")
                    record_id = atoi(input("Enter the id of the record to update:\n"))
                    exit = record_id == "EXIT"
                    filter_by = dict(zip(range(1, len(columns) + 1), columns))
                    print("Choose a column to update:")
                    for k, v in filter_by.items():
                        if v != "id":
                            print(f"{k}) {v}")
                    choose_column = int(input())
                    exit = choose_column == "EXIT"
                    update_column = filter_by[choose_column]
                    if update_column == "id":
                        print("Cannot update the id column.")
                        return
                    new_value = input(f"Enter new value for {update_column}:\n")
                    exit = new_value == "EXIT"
                    query = self.sql_update.format(
                        table=table_name, field=update_column, condition="id = ?"
                    )
                    logging.debug(
                        f"Running:\n{query} with value {new_value} for id {record_id}"
                    )
                    cursor.execute(query, (new_value, record_id))
                    conn.commit()
                    exit = True
                    success = True
                if success:
                    self.search_data(table_name=table_name, id=record_id)
            except Exception as e:
                logging.error(e)

    def delete_data(self, table_name: str):
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                columns = self.get_table_columns(table_name)
                if "id" not in columns:
                    print("No 'id' column found in this table.")
                    return
                record_id = atoi(input("Enter the id of the record to delete:\n"))
                query = self.sql_delete.format(table=table_name, condition="id = ?")
                logging.debug(f"Running:\n{query} for id {record_id}")
                cursor.execute(query, (record_id,))
                conn.commit()
                if cursor.rowcount != 0:
                    print(f"Succesfully deleted from table {table_name}")
                else:
                    print("Cannot find this record in the database")
            except Exception as e:
                logging.error(e)
                print(e)


# def main():
#     name = input("Enter DB name: ")
#     db = DBOperations(name=name)
#     while True:
#         print("\n Menu:")
#         print("**********")
#         print(" 2. Insert data into FlightInfo")
#         print(" 3. Select all data from FlightInfo")
#         print(" 4. Search a flight")
#         print(" 5. Update data some records")
#         print(" 6. Delete data some records")
#         print(" 7. Exit\n")


#     __choose_menu = int(input("Enter your choice: "))
#     db_ops = DBOperations()
#     if __choose_menu == 1:
#         db_ops.create_table()
#     elif __choose_menu == 2:
#         db_ops.insert_data()
#     elif __choose_menu == 3:
#         db_ops.select_all()
#     elif __choose_menu == 4:
#         db_ops.search_data()
#     elif __choose_menu == 5:
#         db_ops.update_data()
#     elif __choose_menu == 6:
#         db_ops.delete_data()
#     elif __choose_menu == 7:
#         exit(0)
#     else:
#         print("Invalid Choice")

if __name__ == "__main__":
    test = DBOperations(name="Testing")
    test.select_all("pilots")
    test.search_data("pilots")
    test.insert_data("pilots")
    test.teardown()
