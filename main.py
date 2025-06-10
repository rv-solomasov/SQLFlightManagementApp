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


class SQLQueries:
    def __init__(self):
        self.sql_create_base = {
            "pilots": """
                CREATE TABLE IF NOT EXISTS Pilots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
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
        self.sql_group = "SELECT COUNT(*), {columns} FROM {table} GROUP BY {columns};"

        self.sql_flight_count = """
            SELECT 
                a.{group_column}, 
                COUNT(fl.id) 
            FROM 
                {table_a} a 
                LEFT JOIN Flights fl 
                ON a.id = fl.{a_id} 
            WHERE {condition}
            GROUP BY a.{group_column};
        """


class DBOperations(SQLQueries):
    def __init__(self, name: str):
        super().__init__()
        self.current_dir = os.path.dirname(os.path.abspath(__file__))
        self.dbname = name.capitalize() + ".db"
        logging.basicConfig(
            filename=f"flight_management_{name}.log",
            level=logging.DEBUG,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )
        if not os.path.exists(self.dbname):
            try:
                self.initialize_tables()
            except Exception as e:
                print("Error, operation aborted")
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
                logging.info(f"Table {table_name} created successfully")
            except KeyError:
                logging.error(f"Cannot find DDL for table {table_name}")
            except Exception as e:
                print("Error, operation aborted")
                logging.error(e)

    def populate_table(self, table_name: str):
        try:
            file_path = os.path.join(
                self.current_dir, "src", f"{table_name.lower()}.csv"
            )
            with open(file_path, "r") as f:
                header = list(map(getattr(str, "lower"), f.readline().split(",")))
                for line in f.readlines():
                    fields = [field.strip() for field in line.strip().split(",")]
                    data = DataClass(table_name, dict(zip(header, fields)))
                    self._insert_data(data)
            logging.info(f"Successfully populated table {table_name}")
        except Exception as e:
            print("Error, operation aborted")
            logging.error(e)

    def drop_table(self, table_name: str):
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                query = self.sql_drop.format(table=table_name.capitalize())
                logging.debug(f"Running:\n{query}")
                cursor.execute(query)
                conn.commit()
                logging.info(f"Table {table_name} dropped successfully")
            except Exception as e:
                print("Error, operation aborted")
                logging.error(e)

    def get_table_columns(self, table_name: str):
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(f"PRAGMA table_info({table_name});")
                return [col[1] for col in cursor.fetchall()]
            except Exception as e:
                print("Error, operation aborted")
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
                cursor.execute(query, data.get_values())
                conn.commit()
                logging.info("Inserted row successfully")
            except Exception as e:
                print("Error, operation aborted")
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
                continue
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
            table_name, id="(SELECT MAX(id) FROM {table})".format(table=table_name)
        )

    def search_data(self, table_name: str, show_all: bool = False, id: int = None):
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                if show_all:
                    query = self.sql_search.format(table=table_name, condition="True")
                elif id is not None:
                    query = self.sql_search.format(
                        table=table_name, condition=f"id = {id}"
                    )
                else:
                    columns = self.get_table_columns(table_name)
                    filter_by = dict(zip(range(1, len(columns) + 1), columns))
                    print("Choose a column to filter by:")
                    for k, v in filter_by.items():
                        print(f"{k}) {v}")
                    choose_column = input()
                    if choose_column.strip().upper() == "EXIT":
                        print("Search cancelled.")
                        return
                    filter_column = filter_by.get(int(choose_column))
                    value = input("Enter a value to filter by:\n")
                    if value.strip().upper() == "EXIT":
                        print("Search cancelled.")
                        return
                    value = atoi(value)
                    query = self.sql_search.format(
                        table=table_name, condition=f"{filter_column} = {value}"
                    )
                logging.debug(f"Running:\n{query}")
                cursor.execute(query)
                data = cursor.fetchall()
                return self.show(table_name=table_name, data=data)
            except Exception as e:
                print("Error, operation aborted")
                logging.error(e)

    def update_data(self, table_name: str):
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                columns = self.get_table_columns(table_name)
                if "id" not in columns:
                    print("No 'id' column found in this table.")
                    return
                print("Enter `EXIT` to cancel.")
                record_id = input("Enter the id of the record to update:\n")
                if record_id.strip().upper() == "EXIT":
                    print("Update cancelled.")
                    return
                record_id = atoi(record_id)
                filter_by = {i: col for i, col in enumerate(columns, 1) if col != "id"}
                print("Choose a column to update:")
                for k, v in filter_by.items():
                    print(f"{k}) {v}")
                choose_column = input()
                if choose_column.strip().upper() == "EXIT":
                    print("Update cancelled.")
                    return
                update_column = filter_by[int(choose_column)]
                new_value = input(f"Enter new value for {update_column}:\n")
                if new_value.strip().upper() == "EXIT":
                    print("Update cancelled.")
                    return
                query = self.sql_update.format(
                    table=table_name, field=update_column, condition="id = ?"
                )
                logging.debug(
                    f"Running:\n{query} with value {new_value} for id {record_id}"
                )
                cursor.execute(query, (new_value, record_id))
                conn.commit()
                print("Record updated successfully.")
                self.search_data(table_name, id=record_id)
            except Exception as e:
                print("Error, operation aborted")
                logging.error(e)

    def delete_data(self, table_name: str):
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                columns = self.get_table_columns(table_name)
                if "id" not in columns:
                    print("No 'id' column found in this table.")
                    return
                record_id = input("Enter the id of the record to delete:\n")
                if record_id.strip().upper() == "EXIT":
                    print("Delete cancelled.")
                    return
                record_id = atoi(record_id)
                query = self.sql_delete.format(table=table_name, condition="id = ?")
                logging.debug(f"Running:\n{query} for id {record_id}")
                cursor.execute(query, (record_id,))
                conn.commit()
                if cursor.rowcount != 0:
                    print(f"Successfully deleted from table {table_name}")
                else:
                    print("Record not found.")
            except Exception as e:
                print("Error, operation aborted")
                logging.error(e)
                print(e)

    def group_data(self, table_name: str):
        columns = self.get_table_columns(table_name)
        if not columns:
            print(f"Table '{table_name}' does not exist or has no columns.")
            return

        filter_by = {i: col for i, col in enumerate(columns, 1) if col != "id"}
        if not filter_by:
            print("No groupable columns found.")
            return

        print("Choose a column to group by (or type 'EXIT' to cancel):")
        for k, v in filter_by.items():
            print(f"{k}) {v}")

        choice = input("Enter choice: ").strip()
        if choice.upper() == "EXIT":
            print("Group by operation cancelled.")
            return

        try:
            choice = int(choice)
            group_column = filter_by.get(choice)
            if not group_column:
                raise ValueError("Invalid column selection.")
        except ValueError:
            print("Invalid selection. Please enter a valid number.")
            return

        query = self.sql_group.format(columns=group_column, table=table_name)
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                logging.debug(f"Running:\n{query}")
                cursor.execute(query)
                data = cursor.fetchall()
                self.show(data=data, headers=["count", group_column])
            except Exception as e:
                print("Error, operation aborted")
                logging.error(f"Failed to group by {group_column}: {e}")

    def flight_summary(self, group_by: str, condition: str = "1=1"):
        if group_by == "Pilot":
            table_a = "Pilots"
            a_id = "pilot_id"
            group_column = "name"
        elif group_by == "Source":
            table_a = "Destinations"
            a_id = "source_id"
            group_column = "airport_code"
        elif group_by == "Destination":
            table_a = "Destinations"
            a_id = "destination_id"
            group_column = "airport_code"
        else:
            print(f"Grouping by '{group_by}' is not supported.")
            return

        query = self.sql_flight_count.format(
            group_column=group_column, table_a=table_a, a_id=a_id, condition=condition
        )

        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                logging.debug(f"Running:\n{query}")
                cursor.execute(query)
                data = cursor.fetchall()
                if data:
                    self.show(headers=[group_column, "NumFlights"], data=data)
                else:
                    print("No summary data found.")
            except Exception as e:
                print("Error, operation aborted")
                logging.error(f"Failed to get flight summary: {e}")
                print(e)

    def select_all(self, table_name: str):
        return self.search_data(table_name=table_name, show_all=True)

    def show(self, data: list[tuple], headers: list = None, table_name: str = None):
        if not headers:
            if table_name:
                headers = self.get_table_columns(table_name)
            else:
                logging.error("Error. No headers or table provided for lookup")
                print("Error visualizing your data")
                return
        print(tabulate(data, headers=headers, tablefmt="grid"))

    def teardown(self):
        if input("Are you sure?(y/n): ") == "y":
            os.remove(os.path.join(self.current_dir, self.dbname))


class DBUI:
    def __init__(self, driver: DBOperations):
        self.driver = driver

    def main_menu(self):
        while True:
            print("\nFlight Management System Menu")
            print("1) Flights")
            print("2) Pilots")
            print("3) Destinations")
            print("4) Flight Summary")
            print("5) Exit")
            choice = input("Select an option: ").strip()
            entites = {"1": "flights", "2": "pilots", "3": "destinations"}
            match choice:
                case "1" | "2" | "3":
                    self.sub_menu(entity=entites[choice])
                case "4":
                    self.flight_summary()
                case "5":
                    print("Exiting...")
                    break
                case _:
                    print("Invalid choice. Please try again.")

    def sub_menu(self, entity: str):
        print(f"\n{entity.capitalize()} Menu")
        print(f"1) Add New {entity.capitalize()}")
        print(f"2) View {entity.capitalize()}")
        print(f"3) Search {entity.capitalize()}")
        print(f"4) Update {entity.capitalize()} Info")
        choice = input("Select an option: ").strip()
        match choice:
            case "1":
                self.driver.insert_data(entity)
            case "2":
                self.driver.search_data(entity, show_all=True)
            case "3":
                self.driver.search_data(entity)
            case "4":
                self.driver.update_data(entity)
            case _:
                print("Invalid choice.")

    def flight_summary(self):
        print("\nFlight Summary")
        print("1) By Pilot")
        print("2) By Source")
        print("3) By Destination")
        group_map = {"1": "Pilot", "2": "Source", "3": "Destination"}
        choice = input("Enter choice (or 'EXIT' to cancel): ").strip()
        if choice.upper() == "EXIT":
            print("Cancelled.")
            return
        group_by = group_map.get(choice)
        if not group_by:
            print("Invalid selection.")
            return
        condition = (
            input("Enter optional WHERE clause (e.g. fl.status = 'on-time'): ").strip()
            or "True"
        )
        self.driver.flight_summary(group_by=group_by, condition=condition)


if __name__ == "__main__":
    ui = DBUI(DBOperations(name=input("Enter project name: ")))
    ui.main_menu()
