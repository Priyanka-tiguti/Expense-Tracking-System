import mysql.connector
from contextlib import contextmanager
from logging_setup import setup_logger

logger=setup_logger('db_helper')

@contextmanager
#Once you COMMIT, the changes are permanent in the database.
def get_db_cursor(commit=False):
    connection=mysql.connector.connect(
        host="localhost",
        user="root",
        password="Priya@2004",
        database="expense_manager",
        use_pure=True
    )
    if connection.is_connected():
        print("✅ Connection successful")
    else:
        print("❌ Connection failed")
    cursor=connection.cursor(dictionary=True)
    yield cursor
    if commit:
        connection.commit()
    
    cursor.close()
    connection.close()



#cursor-->object that execute queries,fetch results and iterate through database
def fetch_all_records():
    with get_db_cursor() as cursor:     #when with get_db_cursor() as cursor: is used, the cursor is automatically closed after the block
        cursor.execute("SELECT * FROM expenses;")
        expenses=cursor.fetchall()
        for expense in expenses:
            print(expense)


def fetch_expenses_for_date(expense_date):
    logger.info(f"fetch_expenses_for_date called with date: {expense_date}")
    with get_db_cursor() as cursor:
        cursor.execute("SELECT * FROM expenses WHERE expense_date=%s",(expense_date,))    #%s->string
        expenses=cursor.fetchall()
        return expenses


def insert_expense(expense_date,amount,category,notes):
    logger.info(f"insert_expense called with date: {expense_date}, amount: {amount}, category: {category}, notes: {notes}")
    with get_db_cursor(commit=True) as cursor:
        cursor.execute(
            "INSERT INTO expenses(expense_date,amount,category,notes) VALUES (%s, %s, %s, %s)",
            (expense_date, amount, category, notes)
        )

def delete_expenses_for_date(expense_date):
    logger.info(f"delete_expenses_for_date called with date: {expense_date}")
    with get_db_cursor(commit=True) as cursor:
        cursor.execute(
            "DELETE FROM expenses WHERE expense_date=%s",
            (expense_date,)
        )

def fetch_expense_summary(start_date,end_date):
    logger.info(f"fetch_expense_summary called with start_date: {start_date}, end_date: {end_date}")
    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT category, SUM(amount) as total FROM expenses WHERE expense_date BETWEEN %s AND %s GROUP BY category",
            (start_date, end_date)
        )
        summary=cursor.fetchall()
        return summary
    
def fetch_monthly_expense_summary():
    logger.info("fetch_expense_summary_by_months")
    with get_db_cursor() as cursor:
        cursor.execute(
            '''
            SELECT MONTH(expense_date) AS expense_month,
            MONTHNAME(expense_date) AS month_name,
            SUM(amount) AS total
            FROM expenses
            GROUP BY expense_month, month_name
            '''
        )
        data = cursor.fetchall()
        
        return data
        

if __name__=='__main__':

    expenses=fetch_expenses_for_date("2024-08-15")
    #print(expenses)
    #summary=fetch_expense_summary("2024-08-01","2024-08-5")
    #for record in summary:
    #    print(record)