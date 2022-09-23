from flypipe.node import node


@node()
def dummy_name_input():
    return 'Chris'


@node(type="spark_sql")
def sql_test():
    return '''
        SELECT
            name
        FROM
            test_schema.test_table
    '''