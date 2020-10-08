"""
Name: Bansari Chauhan
Time To Completion: 20+ hours

Sources:
    1. Used instructor's code of project 2
"""

import string
import random
import copy
from operator import itemgetter
from pprint import  pprint
from functools import cmp_to_key

_ALL_DATABASES = {}
#_INTERM_DB = {}
counter = 0
collation_name = None
collation_function = None

class view(object):
    def __init__(self, output_columns, table_name1, table_name2 = None):
        self.output_columns = output_columns
        self.table_1 = table_name1
        self.table_2 = table_name2

    def return_view_items(self):
        return self.output_columns, self.table_1, self. table_2


class Connection(object):
    def __init__(self, filename, conn_id):
        """
        Takes a filename, but doesn't do anything with it.
        (The filename will be used in a future project).
        """
        self.conn_id = conn_id
        if filename in _ALL_DATABASES:
            self.database = _ALL_DATABASES[filename]
        else:
            self.database = Database(filename)
            _ALL_DATABASES[filename] = self.database

        self.begin_trans = False
        self.commit_trans = True
        self.roll_trans = False
        self.lockstatus = None
        self.view = {}

    def lock_status(self,status):
        self.lockstatus = "EXCLUSIVE"

    def execute(self, statement):
        """
        Takes a SQL statement.
        Returns a list of tuples (empty unless select statement
        with rows to return).
        """

        def create_table(tokens):
            """
            Determines the name and column information from tokens add
            has the database create a new table within itself.
            """
            default_values = {}
            pop_and_check(tokens, "CREATE")
            pop_and_check(tokens, "TABLE")
            table_flag = False
            #print(tokens[0])
            if tokens[0] == "IF":
                #print("---------")
                pop_and_check(tokens, "IF")
                pop_and_check(tokens, "NOT")
                pop_and_check(tokens, "EXISTS")
                table_flag = True
            table_name = tokens.pop(0)
            pop_and_check(tokens, "(")
            column_name_type_pairs = []
            while True:
                column_name = tokens.pop(0)
                column_type = tokens.pop(0)
                if tokens[0] == "DEFAULT":
                    pop_and_check(tokens,"DEFAULT")
                    val = tokens.pop(0)
                    temp_name = table_name + "." + column_name
                    default_values[column_name] = val
                    default_values[temp_name] = val
                assert column_type in {"TEXT", "INTEGER", "REAL"}
                column_name_type_pairs.append((column_name, column_type))
                comma_or_close = tokens.pop(0)
                if comma_or_close == ")":
                    break
                assert comma_or_close == ','
            self.database.create_new_table(table_name, column_name_type_pairs, table_flag, default_values)

        def insert(tokens):
            """
            Determines the table name and row values to add.
            """
            if self.database.Exclusive is not None or self.database.reserved is not None:
                if self.database.Exclusive == self.conn_id or self.database.reserved == self.conn_id:
                    row_columns = []
                    row_contents = []
                    pop_and_check(tokens, "INSERT")
                    pop_and_check(tokens, "INTO")
                    table_name = tokens.pop(0)
                    if self.begin_trans is True:
                        table_new_name = self.database.connection_status(self.conn_id)
                    try:
                        pop_and_check(tokens, "VALUES")
                    except AssertionError:
                        # pop_and_check(tokens,"(")
                        while True:
                            item = tokens.pop(0)
                            row_columns.append(item)
                            comma_or_close = tokens.pop(0)
                            if comma_or_close == ")":
                                break
                            assert comma_or_close == ","
                        pop_and_check(tokens, "VALUES")
                    # print(row_columns)
                    while tokens:
                        pop_and_check(tokens, "(")
                        row_contents = []
                        while True:
                            item = tokens.pop(0)
                            row_contents.append(item)
                            comma_or_close = tokens.pop(0)
                            if comma_or_close == ")":
                                break
                            assert comma_or_close == ','
                        # print(self.begin_trans,self.commit_trans)
                        if self.begin_trans is False:
                            if row_columns:
                                self.database.insert_with_columns(table_name, row_contents, row_columns)
                            else:
                                self.database.insert_into(table_name, row_contents)
                            if tokens:
                                tokens.pop(0)
                        else:
                            self.lockstatus = "EXCLUSIVE"
                            self.database.insert_transaction(table_name, table_new_name)
                            # table_new_name = "table___1"
                            if row_columns:
                                self.database.insert_with_columns(table_new_name, row_contents, row_columns)
                            else:
                                self.database.insert_into(table_new_name, row_contents)
                            if tokens:
                                tokens.pop(0)
                        # _INTERM_DB["table1"] = self.table_1
                        # print(table_1)
                else:
                    #print(self.database.Exclusive,self.database.reserved,"+++++++++++++++")
                    raise AssertionError
            elif self.database.Exclusive is None and self.database.reserved is None:
                row_columns = []
                row_contents = []
                default_flag = False
                pop_and_check(tokens, "INSERT")
                pop_and_check(tokens, "INTO")
                table_name = tokens.pop(0)
                if self.begin_trans is True:
                    table_new_name = self.database.connection_status(self.conn_id)
                if "DEFAULT" not in tokens:
                    try:
                        pop_and_check(tokens, "VALUES")
                    except AssertionError:
                        # pop_and_check(tokens,"(")
                        while True:
                            item = tokens.pop(0)
                            row_columns.append(item)
                            comma_or_close = tokens.pop(0)
                            if comma_or_close == ")":
                                break
                            assert comma_or_close == ","
                        pop_and_check(tokens,"VALUES")
                else:
                    pop_and_check(tokens,"DEFAULT")
                    pop_and_check(tokens, "VALUES")
                    default_flag = True
                    self.database.insert_into(table_name, row_contents, default_flag)
                # print(row_columns)
                while tokens:
                    pop_and_check(tokens, "(")
                    row_contents = []
                    while True:
                        item = tokens.pop(0)
                        row_contents.append(item)
                        comma_or_close = tokens.pop(0)
                        if comma_or_close == ")":
                            break
                        assert comma_or_close == ','
                    # print(self.begin_trans,self.commit_trans)
                    if self.begin_trans is False:
                        if row_columns:
                            self.database.insert_with_columns(table_name, row_contents, row_columns)
                        else:
                            self.database.insert_into(table_name, row_contents)
                        if tokens:
                            tokens.pop(0)
                    else:
                        self.lockstatus = "EXCLUSIVE"
                        self.database.insert_transaction(table_name, table_new_name)
                        # table_new_name = "table___1"
                        if row_columns:
                            self.database.insert_with_columns(table_new_name, row_contents, row_columns)
                        else:
                            self.database.insert_into(table_new_name, row_contents)
                        if tokens:
                            tokens.pop(0)
                    # _INTERM_DB["table1"] = self.table_1
                    # print(table_1)

        def create_view(tokens):
            pop_and_check(tokens,"CREATE")
            pop_and_check(tokens,"VIEW")
            view_name = tokens.pop(0)
            pop_and_check(tokens,"AS")
            self.view[view_name] = copy.deepcopy(tokens)

                    #print(temp_col)
                #print (self.v_1.return_view_items())
                #print("====================",output_columns,table_name)

            #pprint(list(view))




        def view_make_table(tokens):

            #This was just making the new table
            view_name = tokens.pop(0)
            #print(view_name,self.view)
            token = copy.deepcopy(self.view[view_name])
            rows_copy = list(select(token))
            token = copy.deepcopy(self.view[view_name])
            if "JOIN" in token:
                pop_and_check(token, "SELECT")
                output_columns = []
                while True:
                    col = token.pop(0)
                    output_columns.append(col)
                    comma_or_from = token.pop(0)
                    if comma_or_from == "FROM":
                        break
                    assert comma_or_from == ','
                table_name1 = token.pop(0)
                pop_and_check(token, "LEFT")
                pop_and_check(token, "OUTER")
                pop_and_check(token, "JOIN")
                table_name2 = token.pop(0)

                #Write code to retrieve all output_col

                self.v_1 = view(output_columns,table_name1,table_name2)

            else:
                temp_col = []
                pop_and_check(token, "SELECT")
                distinct_flag = False
                if token[0] == "DISTINCT":
                    pop_and_check(token, "DISTINCT")
                    distinct_flag = True
                output_columns = []
                while True:
                    col = token.pop(0)
                    output_columns.append(col)
                    comma_or_from = token.pop(0)
                    if comma_or_from == "FROM":
                        break
                    assert comma_or_from == ','
                table_name = token.pop(0)
                self.v_1 = view(output_columns,table_name)
                for i, col in enumerate(output_columns):
                    if col == "*":
                        temp_col = (self.database.tables[table_name].return_col_names())
                        break

                if temp_col:
                    output_columns.pop(i)
                    output_columns = output_columns[:i] + temp_col + output_columns[i:]
                    output_columns = output_columns[:int(len(output_columns)/2)]
            #col,table_1,table_2 = self.v_1.return_view_items()
            for i in range(len(output_columns)):
                if "." in output_columns[i]:
                    output_columns[i] = output_columns[i].split(".")[1]
            print(output_columns)
            rows = []
            for row in rows_copy:
                temp = {}
                for i, elem in enumerate(row):
                    #print(output_columns[i],row[i])
                    temp[output_columns[i]] = row[i]
                rows.append(temp)
            column_name_type_pairs = []
            for col in output_columns:
                column_name_type_pairs.append((col, None))
            view_table = Table(view_name,column_name_type_pairs,rows)
            return view_table

            #Let's tokenize the rest of tokens now

        def select_aggr(tokens):
            token = []
            condition = {}
            output_columns = []
            token.append(tokens.pop(0))
            while True:
                if tokens[0] == "max" or tokens[0] == "min":
                    max_min = tokens.pop(0)
                    pop_and_check(tokens,"(")
                    col = tokens.pop(0)
                    output_columns.append(col)
                    token.append(col)
                    condition[col] = max_min
                    pop_and_check(tokens,")")
                else:
                    col = tokens.pop(0)
                    output_columns.append(col)
                comma_or_from = tokens.pop(0)
                if comma_or_from == "FROM":
                    break
                token.append(",")
                assert comma_or_from == ","
            token = token + ["FROM"] + tokens
            #print(condition, output_columns, token)
            output_rows = select(token)
            rows = list(zip(*output_rows))
            #print(rows)
            output = []
            solution = []
            for i,col in enumerate(output_columns):
                #print(condition[col],max(rows[i]))
                if condition[col] == "max":
                    output.append(max(rows[i]))
                elif condition[col] == "min":
                    output.append(min(rows[i]))
            solution.append(tuple(output))
            return solution

        def select(tokens):
            """
            Determines the table name, output_columns, and order_by_columns.
            """
            # print(tokens)
            #print(self.conn_id, " conn id++++++++++++++++++++++++++")
            desc_flag = []
            if "min" in tokens or "max" in tokens:
                return select_aggr(tokens)
            if "JOIN" in tokens:
                pop_and_check(tokens, "SELECT")
                output_columns = []
                while True:
                    col = tokens.pop(0)
                    output_columns.append(col)
                    comma_or_from = tokens.pop(0)
                    if comma_or_from == "FROM":
                        break
                    assert comma_or_from == ','
                table_name1 = tokens.pop(0)
                if table_name1 in self.view.keys():
                    tokens.insert(0,table_name1)
                    table_1 = view_make_table(tokens)
                pop_and_check(tokens, "LEFT")
                pop_and_check(tokens, "OUTER")
                pop_and_check(tokens, "JOIN")
                table_name2 = tokens.pop(0)
                if table_name2 in self.view.keys():
                    tokens.insert(0, table_name2)
                    table_2 = view_make_table(tokens)
                pop_and_check(tokens, "ON")
                conditional_column1 = tokens.pop(0)
                pop_and_check(tokens, "=")
                conditional_column2 = tokens.pop(0)
                conditions = []
                conditional_column = None
                if tokens[0] == "WHERE":
                    # print(tokens)
                    pop_and_check(tokens, "WHERE")
                    conditional_column = tokens.pop(0)
                    while tokens[0] != "ORDER" and tokens:
                        conditions.append(tokens.pop(0))
                    # print(conditions,conditional_column)
                pop_and_check(tokens, "ORDER")
                pop_and_check(tokens, "BY")
                order_by_columns = []
                while True:
                    col = tokens.pop(0)
                    order_by_columns.append(col)
                    if not tokens:
                        break
                    if tokens[0] == "DESC":
                        pop_and_check(tokens,"DESC")
                        desc_flag.append(True)
                    else:
                        desc_flag.append(False)
                    if not tokens:
                        break
                    pop_and_check(tokens, ",")
                #print(order_by_columns,desc_flag)
                if self.lockstatus == "EXCLUSIVE":
                    table_new_name = self.database.conn_dict[self.conn_id][0]
                    #print(self.database.tables[table_new_name].name,table_name1,table_name2)
                    if self.database.tables[table_new_name].name == table_name1:
                        table_name1 = table_new_name
                    if self.database.tables[table_new_name].name == table_name2:
                        table_name2 = table_new_name
                    return self.database.select_join(output_columns, table_name1, table_name2, conditional_column1,
                                                     conditional_column2, order_by_columns, conditions, conditional_column, desc_flag)
                else:
                    #print("+++++++++++++++++++",desc_flag)
                    return self.database.select_join(output_columns, table_name1, table_name2, conditional_column1,
                                                     conditional_column2, order_by_columns, conditions,
                                                     conditional_column, desc_flag)

            else:
                if self.database.Exclusive is not None:
                    if self.database.Exclusive != self.conn_id:
                        raise AssertionError
                pop_and_check(tokens, "SELECT")
                distinct_flag = False
                if tokens[0] == "DISTINCT":
                    pop_and_check(tokens, "DISTINCT")
                    distinct_flag = True
                output_columns = []
                collate_columns=[]
                collation_name = None
                while True:
                    col = tokens.pop(0)
                    output_columns.append(col)
                    comma_or_from = tokens.pop(0)
                    if comma_or_from == "FROM":
                        break
                    assert comma_or_from == ','
                table_name = tokens.pop(0)
                if table_name in self.view.keys():
                    tokens.insert(0, table_name)
                    table = view_make_table(tokens)
                    #print(table.return_col_names())
                conditions = []
                conditional_column = None
                if tokens[0] == "WHERE":
                    pop_and_check(tokens, "WHERE")
                    conditional_column = tokens.pop(0)
                    while tokens[0] != "ORDER" and tokens:
                        conditions.append(tokens.pop(0))
                    # print(conditions,conditional_column)
                pop_and_check(tokens, "ORDER")
                pop_and_check(tokens, "BY")
                order_by_columns = []
                while True:
                    col = tokens.pop(0)
                    order_by_columns.append(col)
                    if tokens:
                        if tokens[0] == "COLLATE":
                            collate_columns.append(col)
                            pop_and_check(tokens,"COLLATE")
                            collation_name = tokens.pop(0)
                    
                    if tokens:
                        if tokens[0] == "DESC":
                            pop_and_check(tokens, "DESC")
                            desc_flag.append(True)
                    else:
                        desc_flag.append(False)
                    if not tokens:
                        break
                    pop_and_check(tokens, ",")
                #print(collation_name,collate_columns)
                if self.lockstatus == "EXCLUSIVE":
                    return self.database.select(output_columns, table_name, order_by_columns, conditions, conditional_column,
                                                distinct_flag, desc_flag, self.lockstatus,self.conn_id)
                else:
                    if table_name in self.view.keys():
                        return self.database.select(
                            output_columns, table_name, order_by_columns, conditions, conditional_column, distinct_flag,
                            desc_flag,{} ,None,table,collate_columns,collation_name)
                    else:
                        return self.database.select(
                        output_columns, table_name, order_by_columns, conditions, conditional_column, distinct_flag, desc_flag, {}, None,
                            None,collate_columns,collation_name)

        def delete(tokens):
            pop_and_check(tokens, "DELETE")
            pop_and_check(tokens, "FROM")
            table_name = tokens.pop(0)
            if tokens:
                conditions = []
                pop_and_check(tokens, "WHERE")
                conditional_column = tokens.pop(0)
                while tokens:
                    conditions.append(tokens.pop(0))
                # print(conditions,conditional_column)
                self.database.delete(table_name, conditional_column, conditions)
            else:
                self.database.delete(table_name)

        def update(tokens):
            # conn.execute("UPDATE students SET notes = 'High Grade' WHERE grade > 100;")
            #print(self.conn_id,self.database.reserved,self.database.Exclusive,"+++++++++++++++++++")
            if self.conn_id == self.database.reserved or self.conn_id == self.database.Exclusive:
                columns = []
                values = []
                conditions = []
                conditional_column = None
                pop_and_check(tokens, "UPDATE")
                table_name = tokens.pop(0)
                pop_and_check(tokens, "SET")
                while True:
                    # print(tokens)
                    columns.append(tokens.pop(0))
                    pop_and_check(tokens, "=")
                    values.append(tokens.pop(0))
                    if not tokens or tokens[0] == "WHERE":
                        break
                    pop_and_check(tokens, ",")
                if tokens:
                    pop_and_check(tokens, "WHERE")
                    conditional_column = tokens.pop(0)
                    conditions.append(tokens.pop(0))
                    conditions.append(tokens.pop(0))
                # print(columns,values)
                self.database.update(table_name, columns, values, conditional_column, conditions)
            elif self.database.reserved is None and self.database.Exclusive is None:
                columns = []
                values = []
                conditions = []
                conditional_column = None
                pop_and_check(tokens, "UPDATE")
                table_name = tokens.pop(0)
                pop_and_check(tokens, "SET")
                while True:
                    # print(tokens)
                    columns.append(tokens.pop(0))
                    pop_and_check(tokens, "=")
                    values.append(tokens.pop(0))
                    if not tokens or tokens[0] == "WHERE":
                        break
                    pop_and_check(tokens, ",")
                if tokens:
                    pop_and_check(tokens, "WHERE")
                    conditional_column = tokens.pop(0)
                    conditions.append(tokens.pop(0))
                    conditions.append(tokens.pop(0))
                # print(columns,values)
                self.database.update(table_name, columns, values, conditional_column, conditions)
            else:
                raise AssertionError

        def drop(tokens):
            pop_and_check(tokens, "DROP")
            pop_and_check(tokens, "TABLE")
            table_flag = False
            if tokens[0] == "IF":
                pop_and_check(tokens, "IF")
                pop_and_check(tokens, "EXISTS")
                table_flag = True
            table_name = tokens.pop(0)
            self.database.drop(table_name, table_flag)

        def transactions(tokens,conn_id):
            def trans_begin(tokens,command):
                assert self.begin_trans is False
                self.begin_trans = True
                self.commit_trans = False
                self.roll_trans = False
                if len(tokens) == 2:
                    pass
                else:
                    pop_and_check(tokens,"BEGIN")
                    status = tokens.pop(0)
                    pop_and_check(tokens,"TRANSACTION")
                    if status == "EXCLUSIVE":
                        if self.database.Exclusive is None and self.database.reserved is None:
                            self.database.Exclusive = conn_id
                        else:
                            self.commit_trans = True
                            self.begin_trans = False
                            self.lockstatus = None
                            raise AssertionError
                    if status == "DEFERRED":
                        pass
                    if status == "IMMEDIATE":
                        if self.database.Exclusive is None and self.database.reserved is None:
                            self.database.reserved = conn_id
                        else:
                            self.commit_trans = True
                            self.begin_trans = False
                            self.lockstatus = None
                            raise AssertionError

            def trans_rollback(tokens,command):
                assert self.begin_trans is True
                assert self.commit_trans is False
                assert self.roll_trans is False
                self.roll_trans = True
                self.begin_trans = False
                self.commit_trans = True
                self.lockstatus = None
                #print(self.database.conn_dict[conn_id],"+++++++++++++++")
                if conn_id not in self.database.shared:
                    if conn_id in self.database.conn_dict.keys():
                        for element in self.database.conn_dict[conn_id]:
                            self.database.drop(element)
                        del self.database.conn_dict[conn_id]
                        if conn_id == self.database.reserved:
                            self.database.reserved = None
                        elif conn_id == self.database.Exclusive:
                            self.database.Exclusive = None
                else:
                    self.database.shared.remove(conn_id)


            def trans_commit(tokens,command):
                # print(self.conn_id)
                # print(self.database.shared)
                assert self.commit_trans is False
                assert self.roll_trans is False
                self.commit_trans = True
                self.begin_trans = False
                self.lockstatus = None
                if len(tokens) == 2:
                    if conn_id in self.database.shared:

                        self.database.commit_transaction(self.conn_id)
                        self.database.shared.remove(conn_id)
                    elif conn_id == self.database.reserved:
                        #print("++++++++++++++++",self.database.shared)
                        if not self.database.shared:
                            self.database.commit_transaction(self.conn_id)
                            self.database.reserved = None
                        else:
                            self.begin_trans = True
                            self.commit_trans = False
                            raise AssertionError
                    elif conn_id == self.database.Exclusive:
                        if not self.database.shared:
                            self.database.commit_transaction(self.conn_id)
                            self.database.Exclusive = None
                        else:
                            self.begin_trans = True
                            self.commit_trans = False
                            raise AssertionError
                    elif self.database.reserved is None and self.database.Exclusive is None:
                        pass
                    else:
                        self.begin_trans = True
                        self.commit_trans = False
                        raise AssertionError

            def trans_select(tokens,command):
                assert self.database.Exclusive is None
                if conn_id not in self.database.shared:
                    if conn_id != self.database.reserved:
                        self.database.lock_status.append("shared")
                        self.database.shared.append(conn_id)

            def trans_insert(tokens,command):
                #print(self.database.Exclusive)
                if self.database.Exclusive is not None:
                    #print("in hereeeeeeeeeeeeeeeeeeee")
                    if self.database.Exclusive != conn_id:
                        raise AssertionError
                if self.database.reserved is None and self.database.Exclusive is None:
                    self.database.reserved = conn_id
                    if conn_id in self.database.shared:
                        self.database.shared.remove(conn_id)
                elif self.database.reserved == conn_id:
                    pass
                elif self.database.Exclusive == conn_id:
                    pass
                else:
                    raise AssertionError
                # print("AFTER INSERTING shared list is",self.database.shared)
                # print("AFTER INSERTING shared list is",self.database.shared)

            command = tokens[0]
            if command == "BEGIN":
                trans_begin(tokens,command)
            if command == "COMMIT":
                trans_commit(tokens,command)
            if command == "ROLLBACK":
                trans_rollback(tokens,command)
            if command == "SELECT":
                trans_select(tokens,command)
            if command == "INSERT":
                trans_insert(tokens,command)
            if command == "UPDATE":
                trans_insert(tokens,command)



        tokens = tokenize(statement)
        assert tokens[0] in {"CREATE", "INSERT", "SELECT", "DELETE", "UPDATE", "DROP","BEGIN","COMMIT","ROLLBACK"}
        last_semicolon = tokens.pop()
        assert last_semicolon == ";"

        if tokens[0] == "CREATE":
            if "TABLE" in tokens:
                create_table(tokens)
            else:
                create_view(tokens)
            return []
        elif tokens[0] == "INSERT":
            if self.begin_trans:
                transactions(tokens,self.conn_id)
            insert(tokens)
            return []
        elif tokens[0] == "SELECT":  # tokens[0] == "SELECT"
            if self.begin_trans:
                transactions(tokens,self.conn_id)
            return select(tokens)
        elif tokens[0] == "DELETE":
            return delete(tokens)
        elif tokens[0] == "UPDATE":
            if self.begin_trans:
                transactions(tokens,self.conn_id)
            return update(tokens)
        elif tokens[0] == "DROP":
            return drop(tokens)
        elif tokens[0] == "BEGIN" or tokens[0] == "COMMIT" or tokens[0] == "ROLLBACK":
            return transactions(tokens, self.conn_id)
        assert not tokens

    def executemany(self, statement,values):
        tokens = tokenize(statement)
        #print(tokens)
        row_columns = []
        row_contents = []
        default_flag = False
        pop_and_check(tokens, "INSERT")
        pop_and_check(tokens, "INTO")
        table_name = tokens.pop(0)
        if "DEFAULT" not in tokens:
            try:
                pop_and_check(tokens, "VALUES")
            except AssertionError:
                # pop_and_check(tokens,"(")
                while True:
                    item = tokens.pop(0)
                    row_columns.append(item)
                    comma_or_close = tokens.pop(0)
                    if comma_or_close == ")":
                        break
                    assert comma_or_close == ","
                pop_and_check(tokens, "VALUES")
        else:
            pop_and_check(tokens, "DEFAULT")
            pop_and_check(tokens, "VALUES")
            default_flag = True
            self.database.insert_into(table_name, row_contents, default_flag)
        # print(row_columns)

        pop_and_check(tokens, "(")
        row_content = []
        row_contents = []
        while True:
            item = tokens.pop(0)
            row_content.append(item)
            comma_or_close = tokens.pop(0)
            if comma_or_close == ")":
                break
            assert comma_or_close == ','

        #print(row_content,values)
        for i,row in enumerate(values):
            temp = []
            r = list(row)
            for j in range(len(row_content)):
                if row_content[j] == "?":
                    temp.append(r.pop(0))
                else:
                    temp.append(row_content[j])
            row_contents.append(temp)

        if row_columns:
            for row in row_contents:
                self.database.insert_with_columns(table_name, row, row_columns)
        else:
            for row in row_contents:
                self.database.insert_into(table_name, row)
        if tokens:
            tokens.pop(0)

    def create_collation(self,collation_nam,collation_functio):
        global collation_name
        global collation_function
        collation_name = collation_nam
        collation_function = collation_functio
        #print (self.collation_function("abn","bcd"))

    def close(self):
        """
        Empty method that will be used in future projects
        """
        pass


def connect(filename, timeout=0.0, isolation_level=None):
    """
    Creates a Connection object with the given filename
    """
    global counter
    conn_id = counter + 1
    counter = counter + 1
    return Connection(filename,conn_id)


class Database:
    def __init__(self, filename):
        self.filename = filename
        self.tables = {}
        self.conn_dict = {}
        self.lock_status = []
        self.shared = []
        self.reserved = None
        self.Exclusive = None

    def create_new_table(self, table_name, column_name_type_pairs, table_flag, default_values):
        if table_flag:
            if table_name not in self.tables:
                self.tables[table_name] = Table(table_name, column_name_type_pairs, [], default_values)

        else:
            assert table_name not in self.tables
            self.tables[table_name] = Table(table_name, column_name_type_pairs, [], default_values)
        return []

    def insert_into(self, table_name, row_contents, default_flag = False):
        #print("INSERTING in--------------",table_name)
        assert table_name in self.tables
        table = self.tables[table_name]
        table.insert_new_row(row_contents, default_flag)
        return []

    def insert_with_columns(self, table_name, row_contents, row_columns):
        #print("INSERTING in--------------",table_name)
        assert table_name in self.tables
        table = self.tables[table_name]
        table.insert_new_row_specified(row_contents, row_columns)
        return []

    def select(self, output_columns, table_name, order_by_columns, conditions=None, conditional_columns=[],
               distinct_flag= False, desc_flag = [], lockstatus = None, conn_id=None,table_1=None, collation_columns = [],collation_name=None):
        #print(lockstatus)
        if lockstatus != "EXCLUSIVE":
            if table_1 is not None:
                return table_1.select_rows(output_columns, order_by_columns, conditions, conditional_columns, distinct_flag, desc_flag)
            #print("SELECTING FROM-------------", table_name)
            else:
                assert table_name in self.tables
                table = self.tables[table_name]
                return table.select_rows(output_columns, order_by_columns, conditions, conditional_columns, distinct_flag, desc_flag,collation_columns,collation_name)
        else:
            table_name = self.conn_dict[conn_id][0]
            #print(table_name)
            #print(self.tables)
            #print("SELECTING FROM-------------", table_name)
            table = self.tables[table_name]
            return table.select_rows(output_columns, order_by_columns, conditions, conditional_columns, distinct_flag, desc_flag)


    def delete(self, table_name, conditional_columns=None, conditions=[]):
        assert table_name in self.tables
        table = self.tables[table_name]
        return table.delete(table_name, conditional_columns, conditions)

    def update(self, table_name, columns, values, conditional_column=None, conditions=[]):
        assert table_name in self.tables
        table = self.tables[table_name]
        return table.update(table_name, columns, values, conditional_column, conditions)

    def select_view(self, output_columns, view_name, order_by_columns, conditions, conditional_column, distinct_flag, desc_flag, rows):
        #print(output_columns,view_name,order_by_columns,conditions, conditional_column,distinct_flag,desc_flag)
        pass

    def select_join(self, output_columns, table_name1, table_name2, conditional_column1, conditional_column2,
                    order_by_columns, conditions=[], conditional_columns=None, desc_flag = []):
        assert table_name1 in self.tables
        assert table_name2 in self.tables
        table1 = self.tables[table_name1]
        table2 = self.tables[table_name2]
        rows1 = table1.select_join()
        rows2 = table2.select_join()
        col1 = table1.return_col_names()
        col2 = table2.return_col_names()
        column_names = col1 + col2
        column_name_type_pairs = []
        for col in column_names:
            column_name_type_pairs.append((col, None))
        # print(rows1, rows2)
        output_list = []

        for row1 in rows1:
            flag = False
            for row2 in rows2:
                if row1[conditional_column1] == row2[conditional_column2] and row1[conditional_column1] is not None:
                    row1.update(row2)
                    output_list.append(row1)
                    flag = True
                    # print(flag)

            if flag is False:
                for col in col2:
                    # print(col)
                    row1[col] = None
                output_list.append(row1)
        # pprint(rows1)

        # pprint(output_list)
        # print(column_name_type_pairs)
        new_join_table = Table("new_join_table", column_name_type_pairs, output_list)
        # print(new_join_table.select_join())
        # select_rows(self, output_columns, order_by_columns, conditions=None, conditional_columns=[],
        #                     distinct_flag=False, desc_flag= [])
        distinct_flag = False
        #print(order_by_columns,desc_flag)
        return new_join_table.select_rows(output_columns, order_by_columns, conditions, conditional_columns, distinct_flag, desc_flag)

    def drop(self, table_name, table_flag=False):
        if table_flag:
            if table_name in self.tables:
                del self.tables[table_name]
        else:
            assert table_name in self.tables
            del self.tables[table_name]

    def insert_transaction(self,table_name,table_new_name):
        if table_new_name not in self.tables:
            table1 = self.tables[table_name]
            self.tables[table_new_name] = copy.deepcopy(table1)
        #print("-------------",self.tables)
        #return table1

    def commit_transaction(self,conn_id):
        if conn_id in self.conn_dict.keys():
            for table_new_name in self.conn_dict[conn_id]:
                for table_name in self.tables:
                    if table_new_name == table_name:
                        #print(table_name,"committing ==============")
                        self.tables[self.tables[table_name].name] = copy.deepcopy(self.tables[table_name])

    def connection_status(self,conn_id):
        if conn_id in self.conn_dict.keys() :
            return self.conn_dict[conn_id][0]
        else:
            self.conn_dict[conn_id] = []
            x = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
            self.conn_dict[conn_id].append(x)
            #print(self.conn_dict)
            return x


"""
Make a column copy in this table and check if the logic works
column_name_copy has tablename_column stored
"""


class Table:
    def __init__(self, name, column_name_type_pairs, output_list=[], default_values = {}):
        self.name = name
        # print(column_name_type_pairs)
        self.column_names, self.column_types = zip(*column_name_type_pairs)
        # print(self.column_names,self.column_types)
        self.column_names_copy = []
        self.default_values = default_values
        for col in self.column_names:
            self.column_names_copy.append(self.name + "." + col)
        # self.column_names=self.column_names.extend(self.column_names_copy)
        self.column_names = tuple((list(self.column_names) + self.column_names_copy))
        # print(self.column_names)
        if output_list:
            self.rows = output_list
        else:
            self.rows = []

    def insert_new_row(self, row_contents, default_flag = False):
        #print(default_flag)
        if not default_flag:
            row_contents = (row_contents + row_contents)
            #print(row_contents,self.column_names)
            assert len(self.column_names) == len(row_contents)
            row = dict(zip(self.column_names, row_contents))
            # print(zip(self.column_names, row_contents))
            self.rows.append(row)
        else:
            #print(row_contents)
            for row in self.column_names:
                row_contents.append(self.default_values[row])
                #print(row,self.default_values[row])
            row = dict(zip(self.column_names, row_contents))
            self.rows.append(row)

    def insert_new_row_specified(self, row_contents, row_columns):
        row_contents = row_contents + row_contents
        row_col = copy.deepcopy(row_columns)
        row_columns_copy = []
        try:
            assert len(self.column_names) == len(row_contents)
        except AssertionError:
            row_contents = row_contents[:(len(row_contents) // 2)]
            for row in self.column_names:
                if row not in row_col and "." not in row:
                    #print(row)
                    row_col.append(row)
                    if row in self.default_values.keys():
                        row_contents.append(self.default_values[row])
                    else:
                        row_contents.append(None)
            row_contents = row_contents + row_contents
        for col in row_col:
            row_columns_copy.append(self.name + "." + col)
        #print(row_columns_copy)
        # self.column_names=self.column_names.extend(self.column_names_copy)
        row_col = tuple((list(row_col) + row_columns_copy))
        row = dict(zip(row_col, row_contents))
        self.rows.append(row)

    def select_rows(self, output_columns, order_by_columns, conditions=None, conditional_columns=[],
                    distinct_flag=False, desc_flag= [],collation_columns = [],collation_name = None):
        #pprint(self.rows)
        #print(order_by_columns,collation_columns,collation_name)
        def expand_star_column(output_columns):
            new_output_columns = []
            for col in output_columns:
                if col == "*" or "*" in col:
                    for col in self.column_names:
                        if "." not in col:
                            new_output_columns.append(col)
                else:
                    new_output_columns.append(col)
            return new_output_columns

        def check_columns_exist(columns):
            assert all(col in self.column_names for col in columns)

        def sort_rows(order_by_columns):

            if True in desc_flag:
                if len(order_by_columns) > 1:
                    s = sorted(self.rows, key = itemgetter(order_by_columns[1]), reverse = desc_flag[1])
                    for i, col in enumerate(order_by_columns):
                        #print(col,i,order_by_columns[i],desc_flag[i])
                        if i > 2:
                            s = sorted(s, key = itemgetter(col), reverse = desc_flag[i])
                    s = sorted(s, key = itemgetter(order_by_columns[0]), reverse = desc_flag[0])
                    #pprint(s)
                    return s
                else:
                    return sorted(self.rows, key=itemgetter(*order_by_columns), reverse= True)
            else:
                return sorted(self.rows, key=itemgetter(*order_by_columns))

        def generate_tuples(rows, output_columns, conditions=None, conditional_columns=[]):
            if conditional_columns:
                # print(conditional_columns,conditions)
                assert conditional_columns in self.column_names
                if conditions[0] == ">":
                    for row in rows:
                        # print(row[conditional_columns])
                        if row[conditional_columns] is not None:
                            try:
                                if float(row[conditional_columns]) > float(conditions[1]):
                                    # print(row)
                                    yield tuple(row[col] for col in output_columns)
                            except ValueError:
                                if str(row[conditional_columns]) > str(conditions[1]):
                                    # print(row)
                                    yield tuple(row[col] for col in output_columns)

                if conditions[0] == "<":
                    for row in rows:
                        # print(row[conditional_columns])
                        if row[conditional_columns] is not None:
                            try:
                                if float(row[conditional_columns]) < float(conditions[1]):
                                    # print(row)
                                    yield tuple(row[col] for col in output_columns)
                            except ValueError:
                                # print("row")
                                if str(row[conditional_columns]) < str(conditions[1]):
                                    # print(row)
                                    yield tuple(row[col] for col in output_columns)

                if conditions[0] == "=":
                    for row in rows:
                        # print(row[conditional_columns])
                        if row[conditional_columns] is not None:
                            try:
                                if float(row[conditional_columns]) == float(conditions[1]):
                                    # print(row)
                                    yield tuple(row[col] for col in output_columns)
                            except ValueError:
                                if str(row[conditional_columns]) == str(conditions[1]):
                                    yield tuple(row[col] for col in output_columns)

                if conditions[0] == "IS":
                    if conditions[1] == "NOT":
                        for row in rows:
                            # print(row[conditional_columns])
                            if row[conditional_columns] is not None:
                                # print(row)
                                yield tuple(row[col] for col in output_columns)
                    else:
                        for row in rows:
                            # print(row[conditional_columns])
                            if row[conditional_columns] is None:
                                # print(row)
                                yield tuple(row[col] for col in output_columns)

                if conditions[0] == "!=":
                    for row in rows:
                        # print(row[conditional_columns])
                        if row[conditional_columns] is not None:
                            try:
                                if float(row[conditional_columns]) != float(conditions[1]):
                                    # print(row)
                                    yield tuple(row[col] for col in output_columns)
                            except ValueError:
                                if str(row[conditional_columns]) != str(conditions[1]):
                                    # print(row)
                                    yield tuple(row[col] for col in output_columns)
            else:
                for row in rows:
                    yield tuple(row[col] for col in output_columns)

        # print(self.rows)
        # print(conditions,conditional_columns)
        expanded_output_columns = expand_star_column(output_columns)
        # print(expanded_output_columns)
        # print(distinct_flag,"................")
        check_columns_exist(expanded_output_columns)
        check_columns_exist(order_by_columns)
        sorted_rows = sort_rows(order_by_columns)
        # print(set(list(generate_tuples(sorted_rows, expanded_output_columns,conditions,conditional_columns))))
        #print(order_by_columns,desc_flag,distinct_flag)
        #print(sorted_rows)
        if collation_columns:
            global collation_function
            generated_col = list(generate_tuples(self.rows,expanded_output_columns))
            for i,col in enumerate(order_by_columns):
                new_list = []
                index = []
                if col in collation_columns:
                    sorted_collation = sorted(list(zip(*generated_col))[i],key=cmp_to_key(collation_function))
                    for column in sorted_collation:
                        for j,row in enumerate(generated_col):
                            if column in row and j not in index:
                                index.append(j)
                                new_list.append(row)
                    generated_col = copy.deepcopy(new_list)
                    pprint(generated_col)
                else:
                    print (list(list(zip(*generated_col))[i]))
                    sorted_collation = sorted(list(list(zip(*generated_col))[i]))
                    print (sorted_collation)
                    for column in sorted_collation:
                        for j, row in enumerate(generated_col):
                            if column in row and j not in index:
                                index.append(j)
                                new_list.append(row)
                    generated_col = copy.deepcopy(new_list)
                    pprint (generated_col)


                # s = sorted(self.rows, key = )
        if distinct_flag:
            if conditional_columns:
                return generate_tuples(sorted_rows, expanded_output_columns, conditions, conditional_columns)

            else:

                index = []
                distinct_set = []
                for i, row in enumerate(sorted_rows):
                    # print(row)
                    if row[expanded_output_columns[0]] not in distinct_set:
                        distinct_set.append(row[expanded_output_columns[0]])
                    else:
                        index.append(i)
                for i in reversed(index):
                    sorted_rows.pop(i)
                # print(sorted_rows)
                return generate_tuples(sorted_rows, expanded_output_columns, conditions, conditional_columns)
        else:
            # print(sorted_rows)
            return generate_tuples(sorted_rows, expanded_output_columns, conditions, conditional_columns)

    def delete(self, table_name, conditional_columns=None, conditions=[]):

        if conditions:
            # print(conditions,conditional_columns)
            if conditions[0] == "IS":
                col = []
                # print("in")
                if conditions[1] == "NOT":
                    for (i, row) in enumerate(self.rows):
                        if row[conditional_columns] is not None:
                            # print(row[conditional_columns])
                            col.append(i)
                for i in reversed(col):
                    self.rows.pop(i)
                else:
                    col = []
                    for (i, row) in enumerate(self.rows):
                        if row[conditional_columns] is None:
                            # print(row[conditional_columns])
                            col.append(i)
                    for i in reversed(col):
                        self.rows.pop(i)

            if conditions[0] == ">":
                col = []
                for (i, row) in enumerate(self.rows):
                    # print(i,row)
                    if row[conditional_columns] is not None:
                        if float(row[conditional_columns]) > float(conditions[1]):
                            # print(row)
                            # print(row[conditional_columns])
                            col.append(i)
                for i in reversed(col):
                    self.rows.pop(i)

            if conditions[0] == "<":
                col = []
                for (i, row) in enumerate(self.rows):
                    # print(i, row)
                    if row[conditional_columns] is not None:
                        if float(row[conditional_columns]) < float(conditions[1]):
                            # print(row)
                            # print(row[conditional_columns])
                            col.append(i)
                for i in reversed(col):
                    self.rows.pop(i)

            if conditions[0] == "=":
                col = []
                for (i, row) in enumerate(self.rows):
                    # print(i, row)
                    if row[conditional_columns] is not None:
                        if str(row[conditional_columns]) == str(conditions[1]):
                            # print(row)
                            # print(row[conditional_columns])
                            col.append(i)
                for i in reversed(col):
                    self.rows.pop(i)

            if conditions[0] == "!=":
                col = []
                for (i, row) in enumerate(self.rows):
                    # print(i, row)
                    if row[conditional_columns] is not None:
                        if str(row[conditional_columns]) != str(conditions[1]):
                            # print(row)
                            # print(row[conditional_columns])
                            col.append(i)
                for i in reversed(col):
                    self.rows.pop(i)
        else:
            self.rows = []
        # print(self.rows)

    def update(self, table_name, columns, values, conditional_column=None, conditions=[]):
        assert len(columns) == len(values)
        if conditions:
            if conditions[0] == ">":
                # print("....",columns,values,conditional_column,conditions)
                for i in range(len(columns)):
                    for row in self.rows:
                        try:
                            if float(row[conditional_column]) > float(conditions[1]):
                                row[columns[i]] = values[i]
                        except ValueError:
                            if str(row[conditional_column]) > str(conditions[1]):
                                row[columns[i]] = values[i]

            if conditions[0] == "<":
                # print("....",columns,values,conditional_column,conditions)
                for i in range(len(columns)):
                    for row in self.rows:
                        try:
                            if float(row[conditional_column]) < float(conditions[1]):
                                row[columns[i]] = values[i]
                        except ValueError:
                            if str(row[conditional_column]) < str(conditions[1]):
                                row[columns[i]] = values[i]

            if conditions[0] == "=":
                # print("....",columns,values,conditional_column,conditions)
                for i in range(len(columns)):
                    for row in self.rows:
                        if str(row[conditional_column]) == str(conditions[1]):
                            row[columns[i]] = values[i]

            if conditions[0] == "IS":
                if conditions[1] == "NOT":
                    for i in range(len(columns)):
                        for row in self.rows:
                            if row[conditional_column] is not None:
                                row[columns[i]] = values[i]
                else:
                    for i in range(len(columns)):
                        for row in self.rows:
                            if row[conditional_column] is None:
                                row[columns[i]] = values[i]

            if conditions[0] == "!=":
                # print("....",columns,values,conditional_column,conditions)
                for i in range(len(columns)):
                    for row in self.rows:
                        if str(row[conditional_column]) != str(conditions[1]):
                            row[columns[i]] = values[i]

        else:
            for i in range(len(columns)):
                for row in self.rows:
                    row[columns[i]] = values[i]

    def select_join(self):
        return self.rows

    def return_col_names(self):
        return list(self.column_names)


def pop_and_check(tokens, same_as):
    item = tokens.pop(0)
    assert item == same_as, "{} != {}".format(item, same_as)


def collect_characters(query, allowed_characters):
    letters = []
    for letter in query:
        if letter not in allowed_characters:
            break
        letters.append(letter)
    return "".join(letters)


def remove_leading_whitespace(query, tokens):
    whitespace = collect_characters(query, string.whitespace)
    return query[len(whitespace):]


def remove_word(query, tokens):
    word = collect_characters(query,
                              string.ascii_letters + "_.*" + string.digits)
    if word == "NULL":
        tokens.append(None)
    else:
        tokens.append(word)
    return query[len(word):]


def remove_text(query, tokens):
    assert query[0] == "'"
    query = query[1:]
    end_quote_index = query.find("'")
    text = query[:end_quote_index]
    tokens.append(text)
    query = query[end_quote_index + 1:]
    if query[0] == '\'':
        quote_index = query.find(",", 2)
        text = text + "\'" + query[1:quote_index - 1]
        for i in range(len(text) - 1):
            try:
                if (text[i] == '\'' and text[i + 1] == '\''):
                    text = text[:i] + text[i + 1:]
            except:
                pass
        tokens.pop()
        tokens.append(text)
        query = query[quote_index:]

    return query


def remove_integer(query, tokens):
    int_str = collect_characters(query, string.digits)
    tokens.append(int_str)
    return query[len(int_str):]


def remove_number(query, tokens):
    query = remove_integer(query, tokens)
    if query[0] == ".":
        whole_str = tokens.pop()
        query = query[1:]
        query = remove_integer(query, tokens)
        frac_str = tokens.pop()
        float_str = whole_str + "." + frac_str
        tokens.append(float(float_str))
    else:
        int_str = tokens.pop()
        tokens.append(int(int_str))
    return query


def tokenize(query):
    tokens = []
    while query:
        # print("Query:{}".format(query))
        # print("Tokens: ", tokens)
        old_query = query

        if query[0] in string.whitespace:
            query = remove_leading_whitespace(query, tokens)
            continue

        if query[0] in (string.ascii_letters + "_"):
            query = remove_word(query, tokens)
            continue

        if query[0] in "(),;*>!=?<":
            if query[0] == "!":
                tokens.append("!=")
                query = query[2:]
            else:
                tokens.append(query[0])
                query = query[1:]
            continue

        if query[0] == "'":
            query = remove_text(query, tokens)
            continue

        if query[0] in string.digits:
            query = remove_number(query, tokens)
            continue

        if len(query) == len(old_query):
            raise AssertionError("Query didn't get shorter.")

    return tokens
