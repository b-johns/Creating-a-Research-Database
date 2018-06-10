################################################################################
################################ INTRODUCTION ##################################
################################################################################

# This small, 21-table, easily-deployable database is designed to read in and store
# transcript, demographic, and transfer data on college students. Its most basic
# use case is to identify analytical cohorts of students with particular traits
# (e.g. first time in college) for further modeling and study. Other use cases
# include querying to produce basic descriptive results about different student
# populations.

# Below, the phrase "frozen files" refers to sets of flat file extracts (.csv in this
# case) from a database, each of which represents the database at a particular moment
# in time. A concise database program like this can read in such extracts and begin
# knittting together a coherent picture of these education data over time.


################################################################################
################################ ADMINISTRATIVE ################################
################################################################################

# required libraries
import sqlite3
import os
import csv
import re
from datetime import date

### specify a path to directory containing data to be loaded
data_path = "some_file_path"

### create database connection
db_path = "frozen_file_database.db"
conn = sqlite3.connect(db_path)

### create a cursor object
c = conn.cursor()

### function for extracting all Person IDs in a list of single tuples
def plain_list(tups_list):
    return [x[0] for x in tups_list]


################################################################################
############################### CREATE TABLES ##################################
################################################################################

### create the necessary tables, making sure that each is empty if already extant

### single-field tables
# Enrolled_Course_Credit_Type
c.execute("CREATE TABLE IF NOT EXISTS Enrolled_Course_Credit_Type (Enrolled_Course_Credit_Type_id INTEGER PRIMARY KEY, Enrolled_Course_Credit_Type VARCHAR(128))")
# Person_ID
c.execute("CREATE TABLE IF NOT EXISTS Person_ID (Person_ID_id INTEGER PRIMARY KEY, Person_ID VARCHAR(128))")
# Enrolled_Verified_Grade_J10
c.execute("CREATE TABLE IF NOT EXISTS Enrolled_Verified_Grade_J10 (Enrolled_Verified_Grade_J10_id INTEGER PRIMARY KEY, Enrolled_Verified_Grade_J10 VARCHAR(128))")
# Enrollment_Current_Status
c.execute("CREATE TABLE IF NOT EXISTS Enrollment_Current_Status (Enrollment_Current_Status_id INTEGER PRIMARY KEY, Enrollment_Current_Status VARCHAR(128))")
# Frozen_File_Extract
c.execute("CREATE TABLE IF NOT EXISTS Frozen_File_Extract (Frozen_File_Extract_id INTEGER PRIMARY KEY, Frozen_File_Extract VARCHAR(128))")
# Enrolled_Course_Subject
c.execute("CREATE TABLE IF NOT EXISTS Enrolled_Course_Subject (Enrolled_Course_Subject_id INTEGER PRIMARY KEY, Enrolled_Course_Subject VARCHAR(128))")
# Enrollment_Term
c.execute("CREATE TABLE IF NOT EXISTS Enrollment_Term (Enrollment_Term_id INTEGER PRIMARY KEY, Enrollment_Term VARCHAR(128))")
# Enrollment_Course_Current_Type_J10
c.execute("CREATE TABLE IF NOT EXISTS Enrollment_Course_Current_Type_J10 (Enrollment_Course_Current_Type_J10_id INTEGER PRIMARY KEY, Enrollment_Course_Current_Type_J10 VARCHAR(128))")
# Enrolled_Course_Full_Name_J10
c.execute("CREATE TABLE IF NOT EXISTS Enrolled_Course_Full_Name_J10 (Enrolled_Course_Full_Name_J10_id INTEGER PRIMARY KEY, Enrolled_Course_Full_Name_J10 VARCHAR(128))")
# Person_UIC_ID_J10_id
c.execute("CREATE TABLE IF NOT EXISTS Person_UIC_ID_J10 (Person_UIC_ID_J10_id INTEGER PRIMARY KEY, Person_UIC_ID_J10 VARCHAR(128))")
# Student_Current_Type
c.execute("CREATE TABLE IF NOT EXISTS Student_Current_Type (Student_Current_Type_id INTEGER PRIMARY KEY, Student_Current_Type VARCHAR(128))")
# Person_Gender
c.execute("CREATE TABLE IF NOT EXISTS Person_Gender (Person_Gender_id INTEGER PRIMARY KEY, Person_Gender VARCHAR(128))")
# Person_Race_1
c.execute("CREATE TABLE IF NOT EXISTS Person_Race_1 (Person_Race_1_id INTEGER PRIMARY KEY, Person_Race_1 VARCHAR(128))")
# Person_Ethnic_1
c.execute("CREATE TABLE IF NOT EXISTS Person_Ethnic_1 (Person_Ethnic_1_id INTEGER PRIMARY KEY, Person_Ethnic_1 VARCHAR(128))")
# Person_Address_Zip
c.execute("CREATE TABLE IF NOT EXISTS Person_Address_Zip (Person_Address_Zip_id INTEGER PRIMARY KEY, Person_Address_Zip VARCHAR(128))")
# College_Name
c.execute("CREATE TABLE IF NOT EXISTS College_Name (College_Name_id INTEGER PRIMARY KEY, College_Name VARCHAR(128))")

### multiple-field tables
# Demographic_Entry
c.execute("CREATE TABLE IF NOT EXISTS Demographic_Entry (Demographic_Entry_id INTEGER PRIMARY KEY, Person_ID_id INT, Person_UIC_ID_J10_id INT, Person_Birth_Date DATE, HS_Grad_Date DATE, Person_Address_Zip_id INT, Student_Current_Type_id INT, Person_Gender_id INT, Person_Race_1_id INT, Person_Ethnic_1_id INT, Frozen_File_Extract_id INT, Enrollment_Term_id INT)")
# Course
c.execute("CREATE TABLE IF NOT EXISTS Course (Course_id INTEGER PRIMARY KEY, Enrolled_Course_Name VARCHAR(128), Section_Credit_Value_J10 REAL, Billing_Cred_J10 REAL, Enrolled_Course_Subject_id INT)")
# Course_Instance
c.execute("CREATE TABLE IF NOT EXISTS Course_Instance (Course_Instance_id INTEGER PRIMARY KEY, Enrollment_Term_id INT, Enrollment_Course_Current_Type_J10_id INT, Enrolled_Course_Full_Name_J10_id INT, Enrollment_Term_Start_Date DATE, Course_id INT)")
# Enrollment_Event
c.execute("CREATE TABLE IF NOT EXISTS Enrollment_Event (Enrollment_Event_id INTEGER PRIMARY KEY, Person_ID_id INT, Enrolled_Verified_Grade_J10_id INT, Enrollment_Current_Status_id INT, Course_Instance_id INT, Enrolled_Course_Credit_Type_id INT, Frozen_File_Extract_id INT, Demographic_Entry_id INT)")
# Other_Institution_Enrollment_Event
c.execute("CREATE TABLE IF NOT EXISTS Other_Institution_Enrollment_Event (Other_Institution_Enrollment_Event_id INTEGER PRIMARY KEY, Person_ID_id INT, College_Name_id INT, Enrollment_Begin DATE, Enrollment_End DATE)")



################################################################################
################################## ADD DATA ####################################
################################################################################

### read in list of csv files from specified data path
all_files = os.listdir(data_path)
csv_demographics_files = []
csv_courses_taken_files = []
csv_transfer_files = []
for i in all_files:
    if re.search('demographics', i) and re.search('\.csv$', i):
        csv_demographics_files.append(i)
    elif re.search('courses_taken', i) and re.search('\.csv$', i):
        csv_courses_taken_files.append(i)
    elif re.search('transfer', i) and re.search('\.csv$', i):
        csv_transfer_files.append(i)

### establish dictionary of field abbreviations and sql statements
# abbreviations for field names
f = {
    'pid':'Person ID',
    'evgJ10':'Enrolled Verified Grade (J10)',
    'ecs':'Enrollment Current Status',
    'ecct':'Enrolled Course Credit Type',
    'ffe':'Frozen File Extract',
    'et':'Enrollment Term',
    'ecctJ10':'Enrollment Course Current Type (J10)',
    'ecfnJ10':'Enrolled Course Full Name (J10)',
    'ecn':'Enrolled Course Name',
    'z':'Person Address Zip',
    'uic':'Person UIC ID (J10)',
    'sct':'Student Current Type',
    'g':'Person Gender',
    'r1':'Person Race 1',
    'e1':'Person Ethnic 1',
    'bd':'Person Birth Date',
    'ecsub':'Enrolled Course Subject',
    'scvJ10':'Section Credit Value (J10)',
    'bcJ10':'Billing Cred (J10)',
    'hs':'HS Grad Date',
    'cn':'College Name',
    'eb':'Enrollment Begin',
    'ee':'Enrollment End',
    'rf':'Record Found Y/N',
    'grad?':'Graduated?',
    'etsd':'Enrollment Term Start Date'
}

# SQL to look up item id values
ids = {
        'et':'SELECT Enrollment_Term_id FROM Enrollment_Term WHERE Enrollment_Term = (?)', # Enrollment Term
        'ecctJ10':'SELECT Enrollment_Course_Current_Type_J10_id FROM Enrollment_Course_Current_Type_J10 WHERE Enrollment_Course_Current_Type_J10 = (?)', # Enrollment Course Current Type (J10)
        'ecfnJ10':'SELECT Enrolled_Course_Full_Name_J10_id FROM Enrolled_Course_Full_Name_J10 WHERE Enrolled_Course_Full_Name_J10 = (?)', # Enrolled Course Full Name (J10)
        'ecn':'SELECT Course_id FROM Course WHERE Enrolled_Course_Name = (?)', # Enrolled Course Name
        'de':'SELECT Demographic_Entry_id FROM Demographic_Entry WHERE Person_ID_id = (?) AND Enrollment_Term_id = (?) AND Frozen_File_Extract_id = (?)', # Demographic Entry
        'pid':'SELECT Person_ID_id FROM Person_ID WHERE Person_ID = (?)', # Person ID
        'evgJ10':'SELECT Enrolled_Verified_Grade_J10_id FROM Enrolled_Verified_Grade_J10 WHERE Enrolled_Verified_Grade_J10 = (?)', # Enrolled Verified Grade (J10)
        'ecs':'SELECT Enrollment_Current_Status_id FROM Enrollment_Current_Status WHERE Enrollment_Current_Status = (?)', # Enrollment Current Status (J10)
        'ecct':'SELECT Enrolled_Course_Credit_Type_id FROM Enrolled_Course_Credit_Type WHERE Enrolled_Course_Credit_Type = (?)', # Enrolled Course Credit Type
        'ci':'SELECT Course_Instance_id FROM Course_Instance WHERE Enrollment_Term_id = (?) AND Enrollment_Course_Current_Type_J10_id = (?) AND Enrolled_Course_Full_Name_J10_id = (?) AND Enrollment_Term_Start_Date = (?) AND Course_id = (?)', # Course Instance
        'ffe':'SELECT Frozen_File_Extract_id FROM Frozen_File_Extract WHERE Frozen_File_Extract = (?)', # Frozen File Extract
        'uic':'SELECT Person_UIC_ID_J10_id FROM Person_UIC_ID_J10 WHERE Person_UIC_ID_J10 = (?)', # Person UIC ID (J10)
        'sct':'SELECT Student_Current_Type_id FROM Student_Current_Type WHERE Student_Current_Type = (?)', # Student Current Type
        'g':'SELECT Person_Gender_id FROM Person_Gender WHERE Person_Gender = (?)', # Person Gender
        'r1':'SELECT Person_Race_1_id FROM Person_Race_1 WHERE Person_Race_1 = (?)', # Person Race 1
        'e1':'SELECT Person_Ethnic_1_id FROM Person_Ethnic_1 WHERE Person_Ethnic_1 = (?)', # Person Ethnic 1
        'z':'SELECT Person_Address_Zip_id FROM Person_Address_Zip WHERE Person_Address_Zip = (?)', # Person Address Zip
        'ecsub':'SELECT Enrolled_Course_Subject_id FROM Enrolled_Course_Subject WHERE Enrolled_Course_Subject = (?)', # Enrolled Course Subject
        'ee':'SELECT Enrollment_Event_id FROM Enrollment_Event WHERE Person_ID_id = (?) AND Enrolled_Verified_Grade_J10_id = (?) AND Enrollment_Current_Status_id = (?) AND Enrolled_Course_Credit_Type_id = (?) AND Frozen_File_Extract_id = (?) AND Course_Instance_id = (?) AND Demographic_Entry_id = (?)', # Enrollment Event
        'cn':'SELECT College_Name_id FROM College_Name WHERE College_Name = (?)', # College Name
        'oiee':'SELECT Other_Institution_Enrollment_Event_id FROM Other_Institution_Enrollment_Event WHERE Person_ID_id = (?) AND College_Name_id = (?) AND Enrollment_Begin = (?) AND Enrollment_End = (?)' # Other Institution Enrollment Event
}

# SQL to insert new record
ins = {
        'et':'INSERT INTO Enrollment_Term (Enrollment_Term) VALUES (?)', # Enrollment Term
        'ecctJ10':'INSERT INTO Enrollment_Course_Current_Type_J10 (Enrollment_Course_Current_Type_J10) VALUES (?)', # Enrollment Course Current Type (J10)
        'ecfnJ10':'INSERT INTO Enrolled_Course_Full_Name_J10 (Enrolled_Course_Full_Name_J10) VALUES (?)', # Enrolled Course Full Name (J10)
        'ecn':'INSERT INTO Course (Enrolled_Course_Name, Section_Credit_Value_J10, Billing_Cred_J10, Enrolled_Course_Subject_id) VALUES (?,?,?,?)', # Course/Enrolled Course Name
        'de':'INSERT INTO Demographic_Entry (Person_ID_id, Person_UIC_ID_J10_id, Person_Birth_Date, HS_Grad_Date, Person_Address_Zip_id, Student_Current_Type_id, Person_Gender_id, Person_Race_1_id, Person_Ethnic_1_id, Frozen_File_Extract_id, Enrollment_Term_id) VALUES (?,?,?,?,?,?,?,?,?,?,?)', # Demographic Entry
        'pid':'INSERT INTO Person_ID (Person_ID) VALUES (?)', # Person ID
        'evgJ10':'INSERT INTO Enrolled_Verified_Grade_J10 (Enrolled_Verified_Grade_J10) VALUES (?)', # Enrolled Verified Grade (J10)
        'ecs':'INSERT INTO Enrollment_Current_Status (Enrollment_Current_Status) VALUES (?)', # Enrollment Current Status
        'ecct':'INSERT INTO Enrolled_Course_Credit_Type (Enrolled_Course_Credit_Type) VALUES (?)', # Enrolled Course Credit Type
        'ci':'INSERT INTO Course_Instance (Enrollment_Term_id, Enrollment_Course_Current_Type_J10_id, Enrolled_Course_Full_Name_J10_id, Enrollment_Term_Start_Date, Course_id) VALUES (?,?,?,?,?)', # Course Instance
        'ffe':'INSERT INTO Frozen_File_Extract (Frozen_File_Extract) VALUES (?)', # Frozen File Extract
        'uic':'INSERT INTO Person_UIC_ID_J10 (Person_UIC_ID_J10) VALUES (?)', # Person UIC ID (J10)
        'sct':'INSERT INTO Student_Current_Type (Student_Current_Type) VALUES (?)', # Student Current Type
        'g':'INSERT INTO Person_Gender (Person_Gender) VALUES (?)', # Person Gender
        'r1':'INSERT INTO Person_Race_1 (Person_Race_1) VALUES (?)', # Person Race 1
        'e1':'INSERT INTO Person_Ethnic_1 (Person_Ethnic_1) VALUES (?)', # Person Ethnic 1
        'z':'INSERT INTO Person_Address_Zip (Person_Address_Zip) VALUES (?)', # Person Address Zip
        'ecsub':'INSERT INTO Enrolled_Course_Subject (Enrolled_Course_Subject) VALUES (?)', # Enrolled Course Subject
        'ee':'INSERT INTO Enrollment_Event (Person_ID_id, Enrolled_Verified_Grade_J10_id, Enrollment_Current_Status_id, Enrolled_Course_Credit_Type_id, Frozen_File_Extract_id, Course_Instance_id, Demographic_Entry_id) VALUES (?,?,?,?,?,?,?)', # Enrollment Event
        'cn':'INSERT INTO College_Name (College_Name) VALUES (?)', # College Name
        'oiee':'INSERT INTO Other_Institution_Enrollment_Event (Person_ID_id, College_Name_id, Enrollment_Begin, Enrollment_End) VALUES (?,?,?,?)' # Other Institution Enrollment Event
}


### read, parse, and add in data from demographics frozen files
for i in csv_demographics_files:
    with open(i, "r") as fhand:

        reader = csv.DictReader(fhand)

        for row in reader:
            ### check whether demographic entry already added; iff not, insert it
            try:
                if len(c.execute(ids['de'], (c.execute(ids['pid'], (row[f['pid']],)).fetchone()[0], c.execute(
                    ids['et'], (row[f['et']],)).fetchone()[0], c.execute(ids['ffe'], (row[f['ffe']],)).fetchone()[0])).fetchall()) == 0:
                    check = "insert"
                else:
                    check = "skip_insert"
            except:
                check = "insert"
            if check == "insert":
                ### add Person ID
                if len(c.execute(ids['pid'], (row[f['pid']],)).fetchall()) == 0:
                    c.execute(ins['pid'], (row[f['pid']],))
                    conn.commit()
                else:
                    pass
                ### add Enrollment Term
                if len(c.execute(ids['et'], (row[f['et']],)).fetchall()) == 0:
                    c.execute(ins['et'], (row[f['et']],))
                    conn.commit()
                else:
                    pass
                ### add Person UIC ID (J10)
                if len(c.execute(ids['uic'], (row[f['uic']],)).fetchall()) == 0:
                    c.execute(ins['uic'], (row[f['uic']],))
                    conn.commit()
                else:
                    pass
                ### add Frozen File Extract
                if len(c.execute(ids['ffe'], (row[f['ffe']],)).fetchall()) == 0:
                    c.execute(ins['ffe'], (row[f['ffe']],))
                    conn.commit()
                else:
                    pass
                ### add Student Current Type
                if len(c.execute(ids['sct'], (row[f['sct']],)).fetchall()) == 0:
                    c.execute(ins['sct'], (row[f['sct']],))
                    conn.commit()
                else:
                    pass
                ### add Person Gender
                if len(c.execute(ids['g'], (row[f['g']],)).fetchall()) == 0:
                    c.execute(ins['g'], (row[f['g']],))
                    conn.commit()
                else:
                    pass
                ### add Person Race 1
                if len(c.execute(ids['r1'], (row[f['r1']],)).fetchall()) == 0:
                    c.execute(ins['r1'], (row[f['r1']],))
                    conn.commit()
                else:
                    pass
                ### add Person Ethnic 1
                if len(c.execute(ids['e1'], (row[f['e1']],)).fetchall()) == 0:
                    c.execute(ins['e1'], (row[f['e1']],))
                    conn.commit()
                else:
                    pass
                ### add Person Address Zip
                if len(c.execute(ids['z'], (row[f['z']],)).fetchall()) == 0:
                    c.execute(ins['z'], (row[f['z']],))
                    conn.commit()
                else:
                    pass
                ### add Demographic Entry
                # parsing Person Birth Date for inclusion in the insertion
                bd_date = row[f['bd']].split(' ')[0] # drop any timestamp 0:00
                bd_date_elems = bd_date.split('/')
                bd_year = int(bd_date_elems[2])
                bd_month = int(bd_date_elems[0])
                bd_day = int(bd_date_elems[1])
                # parsing HS Grad Date for inclusion in the insertion
                hs_grad_date = row[f['hs']].split(' ')[0] # drop any timestamp 0:00
                hs_grad_date_elems = hs_grad_date.split('/')
                hs_year = int(hs_grad_date_elems[2])
                hs_month = int(hs_grad_date_elems[0])
                hs_day = int(hs_grad_date_elems[1])
                c.execute(ins['de'], (c.execute(ids['pid'], (row[f['pid']],)).fetchone()[0], c.execute(ids['uic'], (row[f['uic']],)).fetchone()[0], date(bd_year, bd_month, bd_day), date(hs_year, hs_month, hs_day), c.execute(
                    ids['z'], (row[f['z']],)).fetchone()[0], c.execute(ids['sct'], (row[f['sct']],)).fetchone()[0], c.execute(ids['g'], (row[f['g']],)).fetchone()[0], c.execute(
                        ids['r1'], (row[f['r1']],)).fetchone()[0], c.execute(ids['e1'], (row[f['e1']],)).fetchone()[0], c.execute(ids['ffe'], (row[f['ffe']],)).fetchone()[0], c.execute(
                            ids['et'], (row[f['et']],)).fetchone()[0]))
                conn.commit()
    fhand.close()


### read, parse, and add in data from courses_taken frozen files
for i in csv_courses_taken_files:
    with open(i, "r") as fhand:

        reader = csv.DictReader(fhand)

        for row in reader:
            ### check if enrollment event has corresponding demographic entry; iff so, insert enrollment event
            try:
                if len(c.execute(ids['de'], (c.execute(ids['pid'], (row[f['pid']],)).fetchone()[0], c.execute(ids['et'],(row[f['et']],)).fetchone()[0], c.execute(
                    ids['ffe'], (row[f['ffe']],)).fetchone()[0])).fetchall()) != 0:
                     demo_check = "insert"
                else:
                    demo_check = "skip_insert"
            except:
                demo_check = "skip_insert"
            ### check if enrollment event already exists; iff not, insert enrollment event
            try:
                # parse Enrollment Term Start Date to use in checking for existing Course instances
                etsd_date = row[f['etsd']].split(' ')[0] # drop any 0:00 timestamp
                etsd_date_elems = etsd_date.split('/')
                etsd_year = int(etsd_date_elems[2])
                etsd_month = int(etsd_date_elems[0])
                etsd_day = int(etsd_date_elems[1])
                if len(
                    c.execute(ids['ee'],
                        (c.execute(ids['pid'], (row[f['pid']],)).fetchone()[0],
                            c.execute(ids['evgJ10'], (row[f['evgJ10']],)).fetchone()[0],
                                c.execute(ids['ecs'], (row[f['ecs']],)).fetchone()[0],
                                    c.execute(ids['ecct'], (row[f['ecct']],)).fetchone()[0],
                                        c.execute(ids['ffe'], (row[f['ffe']],)).fetchone()[0],
                                            c.execute(ids['ci'], (c.execute(ids['et'], (row[f['et']],)).fetchone()[0], c.execute(ids['ecctJ10'], (row[f['ecctJ10']],)).fetchone()[0], c.execute(ids['ecfnJ10'], (row[f['ecfnJ10']],)).fetchone()[0], date(etsd_year, etsd_month, etsd_day), c.execute(ids['ecn'], (row[f['ecn']],)).fetchone()[0])).fetchone()[0],
                                                c.execute(ids['de'], (c.execute(ids['pid'], (row[f['pid']],)).fetchone()[0], c.execute(ids['et'], (row[f['et']],)).fetchone()[0], c.execute(ids['ffe'], (row[f['ffe']],)).fetchone()[0])).fetchone()[0])).fetchall()) == 0:
                    enroll_check = "insert"
                else:
                    enroll_check = "skip_insert"
            except:
                    enroll_check = "insert"
            ### insert the record iff enrollment event has a corresponding demographic entry for that term and person
            if demo_check == "insert" and enroll_check == "insert":
                ### add Person ID
                if len(c.execute(ids['pid'], (row[f['pid']],)).fetchall()) == 0:
                    c.execute(ins['pid'], (row[f['pid']],))
                    conn.commit()
                else:
                    pass
                ### add Enrolled Course Credit Type
                if len(c.execute(ids['ecct'], (row[f['ecct']],)).fetchall()) == 0:
                    c.execute(ins['ecct'], (row[f['ecct']],))
                    conn.commit()
                else:
                    pass
                ### add Enrolled Verified Grade (J10)
                if len(c.execute(ids['evgJ10'], (row[f['evgJ10']],)).fetchall()) == 0:
                    c.execute(ins['evgJ10'], (row[f['evgJ10']],))
                    conn.commit()
                else:
                    pass
                ### add Enrollment Current Status
                if len(c.execute(ids['ecs'], (row[f['ecs']],)).fetchall()) == 0:
                    c.execute(ins['ecs'], (row[f['ecs']],))
                    conn.commit()
                else:
                    pass
                ### add Frozen File Extract
                if len(c.execute(ids['ffe'], (row[f['ffe']],)).fetchall()) == 0:
                    c.execute(ins['ffe'], (row[f['ffe']],))
                    conn.commit()
                else:
                    pass
                ### add Enrolled Course Subject
                if len(c.execute(ids['ecsub'], (row[f['ecsub']],)).fetchall()) == 0:
                    c.execute(ins['ecsub'], (row[f['ecsub']],))
                    conn.commit()
                else:
                    pass
                ### add Enrolled Course Full Name (J10)
                if len(c.execute(ids['ecfnJ10'], (row[f['ecfnJ10']],)).fetchall()) == 0:
                    c.execute(ins['ecfnJ10'], (row[f['ecfnJ10']],))
                    conn.commit()
                else:
                    pass
                ### add Enrollment Course Current Type (J10)
                if len(c.execute(ids['ecctJ10'], (row[f['ecctJ10']],)).fetchall()) == 0:
                    c.execute(ins['ecctJ10'], (row[f['ecctJ10']],))
                    conn.commit()
                else:
                    pass
                ### add Enrollment Term
                if len(c.execute(ids['et'], (row[f['et']],)).fetchall()) == 0:
                    c.execute(ins['et'], (row[f['et']],))
                    conn.commit()
                else:
                    pass
                ### add Course
                if len(c.execute(ids['ecn'], (row[f['ecn']],)).fetchall()) == 0:
                    c.execute(ins['ecn'], (row[f['ecn']], row[f['scvJ10']], row[f['bcJ10']], c.execute(ids['ecsub'], (row[f['ecsub']],)).fetchone()[0]))
                    conn.commit()
                else:
                    pass
                ### add Course Instance
                if len(c.execute(ids['ci'], (c.execute(ids['et'], (
                    row[f['et']],)).fetchone()[0], c.execute(ids['ecctJ10'], (
                        row[f['ecctJ10']],)).fetchone()[0], c.execute(ids['ecfnJ10'], (row[f['ecfnJ10']],)).fetchone()[0], date(etsd_year, etsd_month, etsd_day), c.execute(ids['ecn'], (row[f['ecn']],)).fetchone()[0])).fetchall()) == 0:
                    c.execute(ins['ci'], (c.execute(ids['et'], (row[f['et']],)).fetchone()[0], c.execute(ids['ecctJ10'], (
                            row[f['ecctJ10']],)).fetchone()[0], c.execute(ids['ecfnJ10'], (row[f['ecfnJ10']],)).fetchone()[0], date(etsd_year, etsd_month, etsd_day), c.execute(ids['ecn'], (row[f['ecn']],)).fetchone()[0]))
                    conn.commit()
                else:
                    pass
                ### add Enrollment Event
                c.execute(ins['ee'],
                    (c.execute(ids['pid'], (row[f['pid']],)).fetchone()[0],
                        c.execute(ids['evgJ10'], (row[f['evgJ10']],)).fetchone()[0],
                            c.execute(ids['ecs'], (row[f['ecs']],)).fetchone()[0],
                                c.execute(ids['ecct'], (row[f['ecct']],)).fetchone()[0],
                                    c.execute(ids['ffe'], (row[f['ffe']],)).fetchone()[0],
                                        c.execute(ids['ci'], (c.execute(ids['et'], (row[f['et']],)).fetchone()[0], c.execute(ids['ecctJ10'], (row[f['ecctJ10']],)).fetchone()[0], c.execute(ids['ecfnJ10'], (row[f['ecfnJ10']],)).fetchone()[0], date(etsd_year, etsd_month, etsd_day), c.execute(ids['ecn'], (row[f['ecn']],)).fetchone()[0])).fetchone()[0],
                                            c.execute(ids['de'], (c.execute(ids['pid'], (row[f['pid']],)).fetchone()[0], c.execute(ids['et'], (row[f['et']],)).fetchone()[0], c.execute(ids['ffe'], (row[f['ffe']],)).fetchone()[0])).fetchone()[0]))
                conn.commit()
    fhand.close()


### read, parse, and add in data from NSC transfer files
for i in csv_transfer_files:
    with open(i, "r") as fhand:

        reader = csv.DictReader(fhand)

        for row in reader:
            ### check that the NSC record is indeed an enrollment event at another college
            if row[f['rf']] == "Y" and row[f['grad?']] == "N" and row[f['cn']] != "Jackson College":
                try:
                    if len(c.execute(ids['oiee'], (c.execute(ids['pid'], row[f['pid']]).fetchone()[0], c.execute(
                        ids['cn'], (row[f['cn']],)).fetchone()[0], row[f['eb']], row[f['ee']])).fetchall()) == 0:
                        check = "insert"
                    else:
                        check = "skip_insert"
                except:
                    check = "insert"
                if check == "insert":
                    ### add Person ID
                    if len(c.execute(ids['pid'], (row[f['pid']],)).fetchall()) == 0:
                        c.execute(ins['pid'], (row[f['pid']],))
                        conn.commit()
                    else:
                        pass
                    ### add College Name
                    if len(c.execute(ids['cn'], (row[f['cn']],)).fetchall()) == 0:
                        c.execute(ins['cn'], (row[f['cn']],))
                        conn.commit()
                    else:
                        pass
                    ### add Other Institution Enrollment Event
                    # parsing Enrollment Begin for inclusion in the insertion
                    eb_year = int(row[f['eb']][0:4])
                    eb_month = int(row[f['eb']][4:6])
                    eb_day = int(row[f['eb']][6:8])
                    # parsing Enrollment End for inclusion in the insertion
                    ee_year = int(row[f['ee']][0:4])
                    ee_month = int(row[f['ee']][4:6])
                    ee_day = int(row[f['ee']][6:8])
                    c.execute(ins['oiee'], (c.execute(ids['pid'], row[f['pid']]).fetchone()[0], c.execute(
                        ids['cn'], (row[f['cn']],)).fetchone()[0], date(eb_year, eb_month, eb_day), date(ee_year, ee_month, ee_day)))
                    conn.commit()
    fhand.close()



################################################################################
################################## RUN QUERIES #################################
################################################################################

# From here, after reading in real data, we could begin running queries for
# all sorts of purposes.
