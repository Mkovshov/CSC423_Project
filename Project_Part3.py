import sqlite3
import pandas as pd

def main():
    print("SUPERMAIDS CLEANING COMPANY - DATABASE IMPLEMENTATION")
    print("=" * 60)
    
    # PART 3a: CREATE DATABASE SCHEMA 
    
    
    db_connect = sqlite3.connect('supermaids.db')
    cursor = db_connect.cursor()
    
    # 1. CLIENT TABLE 
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Client (
        clientNumber INTEGER PRIMARY KEY,
        firstName TEXT NOT NULL,
        lastName TEXT NOT NULL,
        street TEXT,
        city TEXT,
        postCode TEXT,
        telephoneNumber TEXT NOT NULL CHECK (telephoneNumber GLOB '[0-9]*')
    )
    ''')
    
    # 2. EMPLOYEE TABLE
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Employee (
        staffNumber INTEGER PRIMARY KEY,
        firstName TEXT NOT NULL,
        lastName TEXT NOT NULL,
        street TEXT,
        city TEXT,
        postCode TEXT,
        salary REAL NOT NULL CHECK (salary > 0),
        telephoneNumber TEXT NOT NULL CHECK (telephoneNumber GLOB '[0-9]*')
    )
    ''')
    
    # 3. SERVICE_REQUIREMENT TABLE
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Service_Requirement (
        requirementId INTEGER PRIMARY KEY,
        clientNumber INTEGER NOT NULL,
        startDate TEXT NOT NULL,
        startTime TEXT NOT NULL,
        duration INTEGER NOT NULL CHECK (duration > 0),
        comments TEXT,
        FOREIGN KEY (clientNumber) REFERENCES Client(clientNumber)
    )
    ''')
    
    # 4. EQUIPMENT TABLE
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Equipment (
        equipmentId INTEGER PRIMARY KEY,
        usage TEXT,
        cost REAL NOT NULL CHECK (cost >= 0),
        description TEXT NOT NULL
    )
    ''')
    
    # 5. ASSIGNMENT TABLE 
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Assignment (
        staffNumber INTEGER NOT NULL,
        requirementId INTEGER NOT NULL,
        PRIMARY KEY (staffNumber, requirementId),
        FOREIGN KEY (staffNumber) REFERENCES Employee(staffNumber),
        FOREIGN KEY (requirementId) REFERENCES Service_Requirement(requirementId)
    )
    ''')
    
    # 6. REQUIREMENT_EQUIPMENT TABLE 
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Requirement_Equipment (
        requirementId INTEGER NOT NULL,
        equipmentId INTEGER NOT NULL,
        quantity INTEGER NOT NULL CHECK (quantity > 0),
        PRIMARY KEY (requirementId, equipmentId),
        FOREIGN KEY (requirementId) REFERENCES Service_Requirement(requirementId),
        FOREIGN KEY (equipmentId) REFERENCES Equipment(equipmentId)
    )
    ''')
    
    db_connect.commit()
    
    # PART 3b: INSERT SAMPLE DATA 
    
    # Clear any existing data
    cursor.execute("DELETE FROM Requirement_Equipment")
    cursor.execute("DELETE FROM Assignment")
    cursor.execute("DELETE FROM Equipment")
    cursor.execute("DELETE FROM Service_Requirement")
    cursor.execute("DELETE FROM Employee")
    cursor.execute("DELETE FROM Client")
    
    # Insert Clients 
    clients = [
        (1001, 'John', 'Doe', '123 Main St', 'Boston', '02101', '6175551001'),
        (1002, 'Jane', 'Smith', '456 Oak Ave', 'Cambridge', '02138', '6175551002'),
        (1003, 'Robert', 'Johnson', '789 Pine Rd', 'Somerville', '02143', '6175551003'),
        (1004, 'Sarah', 'Williams', '321 Elm St', 'Boston', '02115', '6175551004'),
        (1005, 'Michael', 'Brown', '654 Maple Dr', 'Cambridge', '02139', '6175551005'),
        (1006, 'Emily', 'Davis', '987 Birch Ln', 'Boston', '02108', '6175551006')
    ]
    cursor.executemany('INSERT INTO Client VALUES (?, ?, ?, ?, ?, ?, ?)', clients)
    
    # Insert Employees
    employees = [
        (5001, 'Alice', 'Wilson', '111 First St', 'Boston', '02101', 45000.00, '6175552001'),
        (5002, 'Bob', 'Miller', '222 Second Ave', 'Cambridge', '02138', 42000.00, '6175552002'),
        (5003, 'Carol', 'Taylor', '333 Third Rd', 'Somerville', '02143', 48000.00, '6175552003'),
        (5004, 'David', 'Anderson', '444 Fourth St', 'Boston', '02115', 46000.00, '6175552004'),
        (5005, 'Eva', 'Thomas', '555 Fifth Dr', 'Cambridge', '02139', 44000.00, '6175552005'),
        (5006, 'Frank', 'Jackson', '666 Sixth Ln', 'Boston', '02108', 47000.00, '6175552006')
    ]
    cursor.executemany('INSERT INTO Employee VALUES (?, ?, ?, ?, ?, ?, ?, ?)', employees)
    
    # Insert Service Requirements
    service_reqs = [
        (2001, 1001, '2024-12-09', '07:00', 120, 'Morning cleaning'),
        (2002, 1001, '2024-12-09', '17:00', 120, 'Evening cleaning'),
        (2003, 1002, '2024-12-10', '10:00', 180, 'Weekly deep clean'),
        (2004, 1003, '2024-12-11', '08:00', 90, 'Kitchen cleaning'),
        (2005, 1004, '2024-12-12', '14:00', 60, 'Quick cleanup'),
        (2006, 1005, '2024-12-13', '09:00', 240, 'Full day service')
    ]
    cursor.executemany('INSERT INTO Service_Requirement VALUES (?, ?, ?, ?, ?, ?)', service_reqs)
    
    # Insert Equipment
    equipment = [
        (3001, 'Daily', 1200.50, 'Industrial vacuum cleaner'),
        (3002, 'Weekly', 850.00, 'Floor polisher'),
        (3003, 'As needed', 350.75, 'Carpet cleaner'),
        (3004, 'Daily', 200.00, 'High-pressure washer'),
        (3005, 'Monthly', 1500.00, 'Window cleaning kit'),
        (3006, 'Weekly', 600.25, 'Sanitizing sprayer')
    ]
    cursor.executemany('INSERT INTO Equipment VALUES (?, ?, ?, ?)', equipment)
    
    # Insert Assignments
    assignments = [
        (5001, 2001),
        (5002, 2001),
        (5003, 2002),
        (5004, 2003),
        (5005, 2004),
        (5006, 2005),
        (5001, 2006)
    ]
    cursor.executemany('INSERT INTO Assignment VALUES (?, ?)', assignments)
    
    # Insert Requirement_Equipment
    req_equipment = [
        (2001, 3001, 2),
        (2001, 3002, 1),
        (2002, 3001, 1),
        (2003, 3003, 3),
        (2004, 3004, 1),
        (2005, 3005, 2),
        (2006, 3006, 1)
    ]
    cursor.executemany('INSERT INTO Requirement_Equipment VALUES (?, ?, ?)', req_equipment)
    
    db_connect.commit()
    print("Sample data inserted")
    
    #  PART 3c: 5 SQL QUERIES 
    
    # Query 1: Add a new client
    print("\n1. Add a new client to the system")
    cursor.execute('''
        INSERT INTO Client VALUES (1007, 'Lisa', 'Garcia', '888 Sunset Blvd', 
                                  'Boston', '02110', '6175551007')
    ''')
    print("   Added client 1007: Lisa Garcia")
    
    # Query 2: Record a new service requirement
    print("\n2. Record a new service requirement for a specific client")
    cursor.execute('''
        INSERT INTO Service_Requirement VALUES 
        (2007, 1007, '2024-12-14', '13:00', 90, 'Initial consultation')
    ''')
    print("   Added service requirement 2007 for client 1007")
    
    # Query 3: Insert a new assignment
    print("\n3. Insert a new assignment linking employee to service requirement")
    cursor.execute('INSERT INTO Assignment VALUES (5002, 2007)')
    print("   Assigned employee 5002 to requirement 2007")
    
    # Query 4: Retrieve service requirements for client 1001
    print("\n4. Retrieve service requirements for client 1001")
    cursor.execute('''
        SELECT requirementId, startDate, startTime, duration, comments
        FROM Service_Requirement 
        WHERE clientNumber = 1001
    ''')
    results = cursor.fetchall()
    column_names = [row[0] for row in cursor.description]
    df = pd.DataFrame(results, columns=column_names)
    print(df.to_string(index=False))
    
    # Query 5: Retrieve all service requirements assigned to employee 5001
    print("\n5. Retrieve all service requirements assigned to employee 5001")
    cursor.execute('''
        SELECT sr.requirementId, sr.startDate, sr.startTime, sr.duration, 
               c.firstName, c.lastName
        FROM Service_Requirement sr
        JOIN Assignment a ON sr.requirementId = a.requirementId
        JOIN Client c ON sr.clientNumber = c.clientNumber
        WHERE a.staffNumber = 5001
    ''')
    results = cursor.fetchall()
    column_names = [row[0] for row in cursor.description]
    df = pd.DataFrame(results, columns=column_names)
    print(df.to_string(index=False))
    
    db_connect.commit()
    
    # VERIFICATION
    print("\n" + "=" * 60)
    print("All Table Contents")
    print("=" * 60)
    
    tables = ['Client', 'Employee', 'Service_Requirement', 
              'Equipment', 'Assignment', 'Requirement_Equipment']
    
    for table in tables:
        print(f"\n{table}:")
        cursor.execute(f"SELECT * FROM {table}")
        column_names = [row[0] for row in cursor.description]
        table_data = cursor.fetchall()
        df = pd.DataFrame(table_data, columns=column_names)
        print(df.to_string(index=False))
    
    
    db_connect.close()
    
    print("\n" + "=" * 60)
    print("PROJECT PART 3 COMPLETED SUCCESSFULLY!")
    print("=" * 60)

if __name__ == "__main__":
    main()