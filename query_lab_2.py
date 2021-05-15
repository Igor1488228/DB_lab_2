
import psycopg2
import csv


# підключення до бази даних
def create_connection(db_name, db_user, db_password, db_host):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port="5432"
        )
        print("З'єднання успішне")
    except psycopg2.DatabaseError as e:
        print(f"{e}")
    return connection

conn = create_connection("postgres", "postgres", "postgres", "localhost")
cursor = conn.cursor()



def statistical_query():
    select_query = '''
    SELECT Location.RegName, TestResult.year, avg(TestResult.Ball100)
    FROM migration.TestResult JOIN migration.Participant ON
        migration.TestResult.OutID = migration.Participant.OutID
    JOIN migration.Location ON
        migration.Participant.loc_id = migration.Location.loc_id
    WHERE migration.TestResult.TestName = 'Математика' AND
        migration.TestResult.TestStatus = 'Зараховано'
    GROUP BY migration.Location.RegName, migration.TestResult.year
    '''
    cursor.execute(select_query)

    with open('result2.csv', 'w', encoding="utf-8") as result_csv:
        csv_writer = csv.writer(result_csv)
        header_row = ['Область', 'Рік', 'Середній бал з математики']
        csv_writer.writerow(header_row)
        for row in cursor:
            csv_writer.writerow(row)


statistical_query()



#conn.commit()
cursor.close()
conn.close()