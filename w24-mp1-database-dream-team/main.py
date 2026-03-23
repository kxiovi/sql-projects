'''
CMPUT 291 W24
Mini Project 1: SQL in Python
'''

import sqlite3
from datetime import datetime, date
import sys
import getpass
import os.path

def connect(path):
    global connection, cursor

    connection = sqlite3.connect(path)
    cursor = connection.cursor()
    cursor.execute(' PRAGMA foreign_keys=ON;')
    connection.commit()
    return

def login():
    '''
    reference for getpass(): hfttps://www.geeksforgeeks.org/getpass-and-getuser-in-python-password-without-echo/
    '''

    login_successful = False
    print("\nTo quit, enter <quit>.")
    mem_reg_status = input("Press 1 to sign up (for unregistered users) or press 2 to login in (for registered users). ")
    mem_reg_status = mem_reg_status.strip()
    
    # keep asking until user enters 1 or 2
    while mem_reg_status not in ['1', '2', 'quit']:
        print("\nTo quit, enter <quit>.")
        mem_reg_status = input("Press 1 to sign in (for unregistered users) or press 2 to login in (for registered users). ")
        mem_reg_status = mem_reg_status.strip()
    
    if mem_reg_status == 'quit':
        exit()

    elif mem_reg_status == '2':
        print("Login")
        login_email = input('Email: ').strip()
        login_passwd = getpass.getpass()

        # check if email and passwd match that in members table
        # if they match, then: login_successful = True
        cursor.execute('''
                        SELECT email 
                        FROM members
                        WHERE lower(email) =  ? AND passwd = ?
                        ;
                        ''', (login_email.lower(), login_passwd))
        user_exists = cursor.fetchone()
        if user_exists: 
            login_email = user_exists[0]
            login_successful = True
            print("Logged in.")
            return [login_successful, login_email]
        else: 
            print("Invalid email or password.")

    elif mem_reg_status == '1':
        print("Sign up")
        signup_email = input('Email: ').strip()
        while signup_email == '':
            print("Email cannot be empty.")
            signup_email = input('Email: ').strip()

        # make sure this email is unique!!
        # john@ualberta.ca and JOHN@ualberta.ca are both considered unique
        cursor.execute('''
                        SELECT * 
                        FROM members 
                        WHERE email = ?
                        ;
                        ''', (signup_email,))
        
        not_unique_user = cursor.fetchone()
        # add this info into db (only if email is unique (should be checked above))
        # if this info is added (i.e. email is unique and all the inputs match constrainsts, then: login_successful = True)
        if not_unique_user:
            print("User already exists. Press 2 to login.")
            login_successful = False
        else:
            print("Email available.")
            signup_name = input('Name: ').strip()
            while signup_name == '':
                print("Name cannot be empty.")
                signup_name = input('Name: ').strip()
            signup_byear = input('Birth year: ').strip()
            if signup_byear == '':
                signup_byear = None
            signup_faculty = input('Faculty: ').strip()
            if signup_faculty == '':
                signup_faculty = None
            signup_passwd = getpass.getpass()
            while signup_passwd == '':
                print("Password cannot be empty.")
                signup_passwd = getpass.getpass()
            print(f"{signup_email}, {signup_name}, {signup_byear}, {signup_faculty}")
            cursor.execute('''
                            INSERT INTO members VALUES(?, ?, ?, COALESCE(?, NULL), COALESCE(?, NULL))
                            ;
                            ''', (signup_email, signup_passwd, signup_name, signup_byear, signup_faculty))
            connection.commit()
            login_successful = True
            print("Successfully signed up!")
            return [login_successful, signup_email]

                
    return [login_successful, None]

def memberProfile(email):
    
    
    # PERSONAL INFORMATION
    cursor.execute('''
                    SELECT name, byear, faculty
                    FROM members
                    WHERE email = ?
                    ;
                    
                    ''', (email,))
    
    information = cursor.fetchone()    
    
    name, byear, faculty = information
    print("PERSONAL INFORMATION: ")
    print("Name: " + name)
    print("Birth Year: " + str(byear))
    print("Faculty: " + faculty)
    
    # FINDING BOOKS BORROWED
    cursor.execute('''
                    SELECT COUNT(*) 
                    FROM borrowings
                    WHERE member = ? 
                    AND end_date IS NOT NULL
                    ;
                    
                    ''', (email,))
    
    previousBorrowings = cursor.fetchone()    
    print("Previous Borrowings: " + str(previousBorrowings[0]))
    
    # FINDING CURRENT BORROWINGS
    cursor.execute('''
                    SELECT COUNT(*) 
                    FROM borrowings
                    WHERE member = ? 
                    AND end_date IS NULL
                    ;
                    
                    ''', (email,))
    
    currentBorrowings = cursor.fetchone() 
    print("Current Borrowings: " + str(currentBorrowings[0]))
    
    # FINDING OVERDUE BOOKS
    cursor.execute('''
                    SELECT COUNT(*) 
                    FROM borrowings
                    WHERE member = ? 
                    AND end_date IS NULL
                    AND date('now') > date(start_date, '+20 days')
                    ;
                    
                    ''', (email,))
    
    overdueBorrowings = cursor.fetchone()    
    print("Overdue Borrowings: " + str(overdueBorrowings[0]))    
    
    # PENALTY INFORMATION
    cursor.execute('''
                    SELECT COUNT(*), SUM(COALESCE(amount, 0) - COALESCE(paid_amount, 0))
                    FROM penalties
                    WHERE bid IN (
                        SELECT bid
                        FROM borrowings
                        WHERE member = ?
                    ) AND COALESCE(paid_amount, 0) < amount
                    ;
                    
                    ''', (email,))
    
    numberOfUnpaidPenalties, totalPenaltyAmount = cursor.fetchone()
    if numberOfUnpaidPenalties == None: 
        numberOfUnpaidPenalties = 0
    
    if totalPenaltyAmount == None:
        totalPenaltyAmount = 0

    print("Number of unpaid penalties: " + str(numberOfUnpaidPenalties))
    print("Total User Debt: $" + str(totalPenaltyAmount))

def return_book(email):
    '''
    .days reference: https://stackoverflow.com/questions/151199/how-to-calculate-number-of-days-between-two-given-dates
    datetime general reference: https://stackoverflow.com/questions/32490629/getting-todays-date-in-yyyy-mm-dd-in-python
    strptime vs. strftime reference: https://stackoverflow.com/questions/14619494/how-to-understand-strptime-vs-strftime
    extracting days from timedelta instance: https://stackoverflow.com/questions/25646200/python-convert-timedelta-to-int-in-a-dataframe
    '''

    # current borrowings (list of the bid, title, start_date, return deadline (+20 days))
    cursor.execute('''
                    SELECT b.bid, books.title, b.start_date, date(b.start_date, '+20 days') AS deadline
                    FROM borrowings b
                    JOIN books ON b.book_id = books.book_id
                    WHERE end_date IS NULL AND b.member = ?
                    ;
                   ''', (email,))
    current_borrowings = cursor.fetchall()
    if not current_borrowings:
        print("There are no current borrowings on your account.")
        return
    print("Current borrowings")
    for row in current_borrowings:
        bid, title, start_date, deadline = row
        print(f"BID: {bid}, Title: {title}, Borrowing date: {start_date}, Deadline: {deadline}")
    
    # user can pick which borrowing to return (enter bid in cli)
    bid_to_return = input("Enter bid of the book to return or r to not select any book: ")
    bid_to_return = bid_to_return.strip()
    # error testing
    if bid_to_return == 'r':
        print("No book returned.")
        return
    if not bid_to_return.isdigit():
        print("Your input must be an integer.")
        return
    bid_to_return = int(bid_to_return)
    if bid_to_return not in [row[0] for row in current_borrowings]:
        print("Invalid BID inputted.")
        return
    # done error checking
    # bid_to_return information retrieval
    cursor.execute('''
                    SELECT book_id, start_date, end_date 
                    FROM borrowings 
                    WHERE bid = ?
                    ;
                   ''', (bid_to_return,))
    selected_bid = cursor.fetchone()
    book_id, start_date, end_date = selected_bid

    today = datetime.today()
    
    # system records today's date as returning date
    cursor.execute('''
                    UPDATE borrowings 
                    SET end_date = ?
                    WHERE bid = ? 
                    ;
                    ''', (today.strftime('%Y-%m-%d'), bid_to_return))
    connection.commit()
    print(f"Book {bid_to_return} has been returned on {today.strftime('%Y-%m-%d')}.")

    # $1/day for books returned after deadline. e.g. book returned after 25 days: 25-20=5 ->$5 penalty
    # update penalties
    deadline_date = datetime.strptime(deadline, '%Y-%m-%d').date()
    penalty_amount = date.today() - deadline_date
    penalty_amount = penalty_amount.days
    if penalty_amount > 0:
        # find the last pid value
        cursor.execute('''
                        SELECT max(pid)
                        FROM penalties
                        ;
                        ''')
        prev_pid = cursor.fetchone()
        if prev_pid is None:
            new_pid = 1
        else: 
            new_pid = prev_pid[0] + 1
        cursor.execute('''
                        INSERT INTO penalties
                        VALUES (?, ?, ?, 0)
                        ;
                        ''', (new_pid, bid_to_return, penalty_amount))
        connection.commit()
        print(f"Book {bid_to_return} has been returned with penalty ${penalty_amount}.00.")
    else:
        print(f"Book {bid_to_return} returned.")

    # optional choice to write review by providing a review text & rating (1-5)
    write_review = input(f"Would you like to submit a review for book {bid}? (y/n) ").strip()
    write_review = write_review.lower()
    if write_review == 'y':
        rtext = input("Your review: ")
        rating = input("Your rating, a number fom 1-5 inclusive: ").strip()
        while not int(rating): 
            rating = input("Your rating, a number fom 1-5 inclusive: ").strip()
        rating = int(rating)

        # find the last rid value
        cursor.execute('''
                        SELECT max(rid)
                        FROM reviews
                        ;
                        ''')
        prev_rid = cursor.fetchone()
        if prev_rid is None:
            new_rid = 1
        else: 
            new_rid = prev_rid[0] + 1
        # for each review, a unique rid is recorded, review date is current date & time, member email recorded
        cursor.execute('''
                        INSERT INTO reviews
                        VALUES (?, ?, ?, ?, ?, ?)
                        ;
                       ''',(new_rid, book_id, email, rating, rtext, today.strftime('%Y-%m-%d %H:%M:%S')))
        connection.commit()

def pay_penalty(email):
    # show list of unpaid penalties (any penalty not fully paid)
    cursor.execute('''
                    SELECT p.pid, p.amount - COALESCE(p.paid_amount,0) AS unpaid_penalty, b.book_id, books.title
                    FROM penalties p
                    JOIN borrowings b ON p.bid = b.bid
                    JOIN books ON b.book_id = books.book_id
                    WHERE b.member = ? AND COALESCE(p.paid_amount,0) < p.amount
                    ;
                   ''',  (email,))
    unpaid_penalties_list = cursor.fetchall()
    if not unpaid_penalties_list:
        print("No unpaid penalties.")
    else:
        print('Unpaid penalties list: ')
        for row in unpaid_penalties_list:
            pid, unpaid_penalty, book_id, title = row
            print(f"PID: {pid}, Book ID: {book_id}, Title: {title}, Unpaid penalty: ${unpaid_penalty:.2f}")
        # user can select a penalty
        pay_penalty = input("Enter PID of penalty to pay, or r to return: ").strip()
        pay_penalty = pay_penalty.lower()
        if pay_penalty == 'r':
            print("No penalty paid.")
        else:
            if not pay_penalty.isdigit():
                print("PID must be integer.")
                return
            if int(pay_penalty) not in [row[0] for row in unpaid_penalties_list]:
                print("The PID entered must be valid. Check unpaid penalties list.")
            else:
                # user can then pay that penalty partially or in full
                cursor.execute('''
                                SELECT bid, amount, paid_amount 
                                FROM penalties 
                                WHERE pid = ?
                                ;
                               ''', (pay_penalty,))
                penalty_in_db = cursor.fetchone()
                spec_bid, spec_amount, spec_paid_amount = penalty_in_db
                if spec_paid_amount == None:
                    spec_paid_amount = 0
                debt = spec_amount - spec_paid_amount
                user_wants_to_pay = input(f"Enter amount to pay. You can pay maximum {debt}: ").strip()
                try:
                    user_wants_to_pay = float(user_wants_to_pay)
                except ValueError:
                    print("Invalid format.")
                    return
                user_wants_to_pay = float(user_wants_to_pay)
                if 0 < user_wants_to_pay <= debt:
                    updated_paid_amount = user_wants_to_pay + spec_paid_amount
                    cursor.execute('''
                                    UPDATE  penalties
                                    SET paid_amount = ?
                                    WHERE pid = ?
                                    ;
                                   ''', (updated_paid_amount, pay_penalty))
                    connection.commit()
                else:
                    print("Please pay the maximum only.")


def search_books(email, keyword, page):
    '''
    Search for books with the specific keyword.
    Page param allows for pagination.
    '''
    offset = (page - 1) * 5

    query = """
        SELECT b.book_id, b.title, b.author, b.pyear, AVG(r.rating) AS avg_rating, 
               CASE 
                    WHEN MAX(bor.start_date) IS NOT NULL AND bor.end_date IS NOT NULL THEN 'Available'
                    WHEN bor.start_date IS NULL AND bor.end_date IS NULL THEN 'Available'
                    ELSE 'Unavailable'
               END as availability
        FROM books b
        LEFT JOIN reviews r ON b.book_id = r.book_id
        LEFT JOIN borrowings bor ON b.book_id = bor.book_id
        WHERE lower(b.title) LIKE ? OR lower(b.author) LIKE ?
        GROUP BY b.book_id, b.title, b.author, b.pyear
        ORDER BY
            CASE WHEN lower(b.title) LIKE ? THEN 0 ELSE 1 END,
            CASE WHEN lower(b.title) LIKE ? THEN b.title ELSE b.author END
        LIMIT 5 OFFSET ?
        """
    cursor.execute(query, ('%' + keyword + '%', '%' + keyword + '%', '%' + keyword + '%', '%' + keyword + '%', offset))

    books = cursor.fetchall()
    if books:
        for book in books:
            print("Book ID:", book[0],"| Title:", book[1],"| Author:", book[2],"| Publish Year:", book[3], "| Average Rating:", book[4] if book[4] else "No Ratings!","| Availability:", book[5],"\n")
            print("*---------------------------------------------------------------------------------------------------------------------------------------------*\n")
        more = input("Enter 'm' to see more results. Click 'b' to borrow a book. Click 'e' to exit: ").strip().lower()
        print("\n")
        if more == 'm':
            return True
        elif more == 'b':
            book = int(input("Enter the book_id of the book you want to borrow: "))
            borrow_book(email,book)
            return 'e'
        elif more == 'e':
            return 'e'
        else:
            print("Invalid Command!!!")
            return 'e'
    else:
        print("No books found with the given keyword or no more books to display ")
        return 'e'

def borrow_book(email, book_id):
    '''
    Borrow book using book_id. 
    '''
    
    cursor.execute("SELECT book_id from books")
    ids = [row[0] for row in cursor.fetchall()]
    if book_id not in ids:
        print("Invalid book ID!!!")
        return
    cursor.execute('''SELECT bor.bid,
    CASE 
        WHEN (bor.start_date) IS NOT NULL AND bor.end_date IS NULL THEN 'Borrowed'
        ELSE 'Available'
    END as availability
    FROM books b
    LEFT JOIN borrowings bor on b.book_id = bor.book_id
    WHERE b.book_id = ?
    ORDER BY start_date;
                    ''', (book_id,))
    
    record = cursor.fetchall()
    length = len(record)
    if length > 1:
        record = record[-1]
    else:
        record = record[0]
    if record is not None:
        if record[1]=="Available":
            today = datetime.today().strftime('%Y-%m-%d')
            cursor.execute('''SELECT max(bid) FROM borrowings;''')
            bid = cursor.fetchone()[0]
            
            if bid is None:
                bid = 1
            else:
                bid = bid + 1
            cursor.execute("INSERT INTO borrowings (bid,member, book_id, start_date) VALUES (?, ?, ?, ?)",
                        (bid,email, book_id, today))
            connection.commit()
            print("Book borrowed successfully.")
        else:
            print("Sorry, this book is currently not available for borrowing.")
        
    
def main():
    '''
    global connection, cursor, sessionexists
    sessionexists = False

    if len(sys.argv) < 2:
        database_name = input("Please enter the name of your database file: ")
    else:
        database_name = sys.argv[1]


    connect(path)
    '''

    global connection, cursor

    if len(sys.argv) != 2:
        print("Error. You must pass in location(name) of database file.")
        return
    database_name = sys.argv[1]

    file_found = False  
    while not(file_found):
        FileNameExists = os.path.exists(database_name)
        FileName_EndsWith_db = database_name.endswith("db")
        if not(FileNameExists and FileName_EndsWith_db):
            print ("Database file not found. Please enter correct file name. ")
            database_name = input("Enter the name of database: ")
        else:
            print("\nDatabase found and created.")
            file_found = True
            
    name = "./"
    name += database_name
    path = name
    #database_file = sys.argv[1]    
    connect(path)
    #connection.commit()

    # login
    logged_in, email = login()
    while email == None or logged_in == False: 
        logged_in, email = login()
    

    # program
    while logged_in:
        print("\nHelp Menu")
        print("\tPress 1 to view member profile.")
        print("\tPress 2 to return book.")
        print("\tPress 3 to search for / return books.")
        print("\tPress 4 to pay penalty.")
        print("\tPress 5 to sign out.")
        print("\tPress 6 to exit program.")

        button = input("\nWhat would you like to do? ").strip()
        if not button.isdigit():
            print("Your input must be an integer (see Help Menu).")
            continue
        button = int(button)

        if button == 1:
            memberProfile(email)
        elif button == 2:
            return_book(email)
        elif button == 3:
            keyword = input("Enter keyword to search: ").lower()
            page = 1
            while True:
                if not keyword.isspace():
                    action = search_books(email, keyword, page)
                    if action:
                        page +=1
                    if action == 'e':
                        break
                else:
                    action = search_books(email, '', page)
                    if action:
                        page +=1
                    if action == 'e':
                        break
        elif button == 4:
            pay_penalty(email)
        elif button == 5:
            print("You have signed out.")
            email = None; logged_in = False
            while email == None or logged_in == False: 
                logged_in, email = login()
        elif button == 6:
            print("You have exited the program.")
            return
        else:
            print("Invalid. Choose a valid option from Help Menu.")
            
    connection.close()
    return

if __name__ == "__main__":
    main()
