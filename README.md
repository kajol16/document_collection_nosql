# Document collection nosql
Use MongoDB as an example of a document-oriented NOSQL system, and see how data is stored and queried in such a system. You will also learn about the difference between storing data in a flat (relational) format versus in a document (complex object) JSON or XML format.  

The input to your program will be data files in flat relational format (text files in .csv format - comma separated values) for the COMPANY database from the textbook. The schemas for this data are the same as for the COMPANY database in the textbook() in chapters 5 and 6 for the tables DEPARTMENT, EMPLOYEE, PROJECT, and WORKS_ON. Design three document collections (complex objects) corresponding to this data and store each as a document collection in MongoDB:     

The PROJECTS document collection will store a collection of PROJECT documents. Each PROJECT document will include the following data about each PROJECT object (document): PNAME, PNUMBER, DNAME (for the controlling DEPARTMENT), and a collection of the workers (EMPLOYEES) who work on the project. This will be nested within the PROJECT object (document) and will include for each worker: EMP_LNAME, EMP_FNAME, HOURS. 

The EMPLOYEES document collection will store a collection of EMPLOYEE documents. Each EMPLOYEE document will include the following data about each EMPLOYEE object (document): EMP_LNAME, EMP_FNAME, DNAME (department where the employee works), and a collection of the projects that the employee works on. This will be nested within the EMPLOYEE object (document) and will include for each project: PNAME, PNUMBER, HOURS. 

The DEPARTMENTS document collection will store a collection of DEPARTMENT documents. Each DEPARTMENT document will include the following data about each DEPARTMENT object (document): DNAME, MANAGER_LNAME (the last name of the employee who manages the department), MGR_START_DATE, and a collection of the employees who work for that department. This will be nested within the DEPARTMENT object (document) and will include for each employee: E_LNAME, E_FNAME, SALARY.


# Programming Language used: 

Python 3.8


# How to execute:
Create the folder structure Main Folder-> python application(toxml and tojson files), input folder(store input files inside input folders), output folder(create an output folder for output files)
Install requirements.txt
Run .py file: tojson.py or toxml.py
Command prompt run command python3 tojson.py or toxml.py

