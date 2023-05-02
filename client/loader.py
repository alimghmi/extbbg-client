from database import connection


def get_tickers():
    schema = 'etl'
    table = "v_extbbg_pcs"
    db_client = connection.MSSQLDatabase()
    _ = db_client.__getcolumn__(schema=schema, 
                                    table_name=table,
                                        column_name=['isin', 'pcs_all'])                          
    return {row[0]: row[1] for row in _}
