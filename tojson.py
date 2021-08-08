
import csv
import json
from pymongo import MongoClient #installed python driver for making a connection with mongodb
from pymongo.errors import DuplicateKeyError


#Connect to mongodb
client = MongoClient('localhost:27017')
database = client.project2
database1 = client.project1


def projectDocumentcollection():

    rows = createProjectDocument()
    data = {}
    data['Projects'] = []
    for lines in rows:
        try:
            database.projects.insert_one(
                lines
            )
        except (DuplicateKeyError):
            print()

        data['Projects'].append(lines)
    # Serializing json
    json_object = json.dumps(data, indent=4)

    # Writing Project.json in output folder
    with open("output/Projects.json", "w") as outfile:
        outfile.write(json_object)

#This is the function to make PROJECT document collection
#Using this function we are writing mongodb query to join data from project document collection and department document collection

def createProjectDocument():

    results=database1.project.aggregate([
        {"$match": {}},
        {"$lookup": {
            "from": "department",
            "localField": "Dnumber",
            "foreignField": "Dnumber",
            "as": "R"
        }},
        {"$unwind": "$R"},

        {"$lookup": {
            "from": "works_on",
            "localField": "Pnumber",
            "foreignField": "Pnumber",
            "as": "S"
        }},
        {"$unwind": "$S"},
        {"$lookup": {
            "from": "employee",
            "localField": "S.ESSN",
            "foreignField": "SSN",
            "as": "Q"
        }},
        {"$unwind": "$Q"},

        {"$group": {"_id": "$Pnumber","Pname": {"$first":"$Pname"},"Dname": {"$first":"$R.Dname"},
                    "Workers": {"$push": {"Lname": "$Q.Lname", "Fname": "$Q.Fname",  "Hours": "$S.hours"}}}},
    ])
    return results
   

def employeeDocumentcollection():

    rows = createEmployeeDocument()
    data = {}
    data['Employees'] = []
    for lines in rows:
        try:
            database.employees.insert_one(
                lines
            )
        except (DuplicateKeyError):
            print()
        data['Employees'].append(lines)
    # Serializing json
    json_object = json.dumps(data, indent=4)

     # Writing Employees.json in output folder
    with open("output/Employees.json", "w") as outfile:
        outfile.write(json_object)

#This is the method to create EMPLOYEE document
#Using this function we are writing mongodb query to join data from department document collection and project document collection

def createEmployeeDocument():

    results=database1.employee.aggregate([
        {"$match": {}},
        
        {"$lookup": {
            "from": "works_on",
            "foreignField": "ESSN",
            "localField": "SSN",
            "as": "W"
        }},
        {"$unwind": "$W"},
        
        {"$lookup": {
            "from": "project",
            "foreignField": "Pnumber",
            "localField": "W.Pnumber",
            "as": "P"
        }},
        {"$unwind": "$P"},

        {"$lookup": {
            "from": "department",
            "foreignField": "Dnumber",
            "localField": "Dnumber",
            "as": "D"
        }},
        {"$unwind": "$D"},

        {"$group": {"_id": "$SSN","EMP_Fname": {"$first":"$Fname"},"EMP_Lname": {"$first":"$Lname"},"Dname": {"$first":"$D.Dname"},
                    "projects": {"$push": {"Pnumber": "$W.Pnumber", "Pname": "$P.Pname", "Hours": "$W.hours"}}}},
    ])
    return results


def departmentsDocumentcollection():
    rows = createDepartmentsDocument()
    data = {}
    data['Departments'] = []
    for lines in rows:
        try:
            database.departments.insert_one(
                lines
            )
        except (DuplicateKeyError):
            print()
        data['Departments'].append(lines)
    # Serializing json
    json_object = json.dumps(data, indent=4)

     # Writing Department.json in output folder
    with open("output/Departments.json", "w") as outfile:
        outfile.write(json_object)

#This is the method to create department document
#Using this function we are writing mongodb query to join data from employee document collection and department document collection

def createDepartmentsDocument():

    results=database1.department.aggregate([
        {"$match": {}},
        
        {"$lookup": {
            "from": "employee",
            "foreignField": "Super_SSN",
            "localField": "mgr_ssn",
            "as": "E"
        }},
        {"$unwind": "$E"},

        {"$lookup": {
            "from": "employee",
            "foreignField": "SSN",
            "localField": "mgr_ssn",
            "as": "M"
        }},
        {"$unwind": "$M"},

        {"$lookup": {
            "from": "department",
            "foreignField": "Mgr_ssn",
            "localField": "Mgr_start_date",
            "as": "D"
        }},
        {"$unwind": "$D"},

        {"$group": {"_id": "$Dnumber","Dname": {"$first":"$Dname"},"MGR_Lname": {"$first":"$M.Lname"},"MGR_Fname": {"$first":"$M.Fname"},"Mgr_start_date": {"$first":"$D.mgr_start_date"},
                    "employees": {"$push": {"E_Lname": "$E.Lname","E_Fname": "$E.Fname", "Salary": "$E.Salary"}}}},

    ])

    return results


def executeQuery():
    query1=database.projects.find({'Dname': 'Software'},{"Dname":1,"worker":{ "$slice": -1 }})
    print("""query1=database.projects.find({'Dname': 'Software'},{"Dname":1,"worker":{ "$slice": -1 }})""")
    for querys in query1:
        print(querys)
    print()
    query2=database.departments.find({'Dname': 'HR'},{"Dname":1,"worker":1})
    print("""query2=database.departments.find({'Dname': 'HR'},{"Dname":1,"worker":1})""")
    for querys in query2:
        print(querys)
    print()
    query3 = database.departments.find({'Dname': 'Sales'},{"Pname":1,"Dname":1,"worker":{ "$slice": -1 }})
    print("""query3 = database.projects.find({'Dname': 'Sales'},{"Pname":1,"Dname":1,"worker":{ "$slice": -1 }})""")
    for querys in query3:
        print(querys)
    print()
    query4 = database.employees.find({'EMP_Fname': 'Kim', "Dname": "Software"})
    print("""query4 = database.employees.find({'EMP_Fname': 'Kim', "Dname": "Software"})""")
    for querys in query4:
        print(querys)
    print()
    query5 = database.employees.find({'projects.Pname': 'MotherBoard', "projects.Hours":{"$gte": 5}})
    print("""query5 = database.employees.find({'projects.Pname': 'MotherBoard', "projects.Hours":{"$gte": 5}})""")
    for querys in query5:
        print(querys)
    print()
    query6=database.projects.find({'Dname': 'Administration'},{"Dname":1,"worker":1})
    print("""query1=database.projects.find({'Dname': 'Administration'},{"Dname":1,"worker":1})""")
    for querys in query6:
        print(querys)
    print()
def main():

    
    file_csv = open('input/PROJECT.txt', 'r')
    reader_csv = csv.reader(file_csv, delimiter=',', quotechar="'",skipinitialspace = True)
    for lines in reader_csv:
        try:
            database1.project.create_index('Pnumber', unique=True)
            database1.project.insert_one(
                {
                    "Pname": (lines[0]),
                    "Pnumber": int(lines[1]),
                    "Dlocation": (lines[2]),
                    "Dnumber": int(lines[3])

                }
            )
        except (DuplicateKeyError):
            print()

    file_csv = open('input/EMPLOYEE.txt', 'r')
    reader_csv = csv.reader(file_csv, delimiter=',', quotechar="'",skipinitialspace = True)
    for lines in reader_csv:
        try:
            database1.employee.create_index('SSN', unique=True)
            database1.employee.insert_one(
                {
                    "Fname": (lines[0]),
                    "Minit": (lines[1]),
                    "Lname": (lines[2]),
                    "SSN": (lines[3]),
                    "Bdate": (lines[4]),
                    "Address": (lines[5]),
                    "Sex": (lines[6]),
                    "Salary": int(lines[7]),
                    "Super_SSN":(lines[8]),
                    "Dnumber": int(lines[9])

                }
            )
        except (DuplicateKeyError):
            print()

    file_csv = open('input/DEPARTMENT.txt', 'r')
    reader_csv = csv.reader(file_csv, delimiter=',', quotechar="'",skipinitialspace = True)
    for lines in reader_csv:
        try:
            database1.department.create_index('Dnumber', unique=True)
            database1.department.insert_one(
                {
                    "Dname": str(lines[0]),
                    "Dnumber": int(lines[1]),
                    "mgr_ssn": (lines[2]),
                    "mgr_start_date": str(lines[3])
                }
            )
        except (DuplicateKeyError):
            print()

    file_csv = open('input/WORKS_ON.txt', 'r')
    reader_csv = csv.reader(file_csv, delimiter=',', quotechar="'",skipinitialspace = True)
    for lines in reader_csv:
        try:
            database1.works_on.create_index([('ESSN',1),('Pnumber',1)], unique=True)
            database1.works_on.insert_one(
                {
                    "ESSN": (lines[0]),
                    "Pnumber": int(lines[1]),
                    "hours":float(lines[2])
                }
            )
        except (DuplicateKeyError):
            print()


    projectDocumentcollection()
    employeeDocumentcollection()
    departmentsDocumentcollection()  
    executeQuery()


if __name__ == '__main__':
    main()
