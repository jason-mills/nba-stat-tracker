import pandas as pd
import psycopg2

import secret

def execute_query(query_script, query_values=None, return_result=True):
    connection = psycopg2.connect(database=secret.database_name, host=secret.database_host, user=secret.database_user, password=secret.database_pass, port=secret.database_port)
    connection.autocommit=True
    cursor = connection.cursor()

    results = []

    try:
        

        if query_values is None:
            cursor.execute(query_script)
        else:
            cursor.execute(query_script, query_values)

        if return_result:
            column_names = [description[0] for description in cursor.description]
            for row in cursor:
                results.append(row)

            results = pd.DataFrame(results)
            if not results.empty:
                results.columns = column_names

        cursor.close()
        connection.close()

        return True, results

    except Exception as error:
        print(error)

        cursor.close()
        connection.close()
        
        return False, pd.DataFrame(results)
