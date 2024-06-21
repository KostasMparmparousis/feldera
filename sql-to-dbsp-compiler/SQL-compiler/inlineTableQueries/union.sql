CREATE TABLE Departments (
    DepartmentID INT PRIMARY KEY,
    DepartmentName VARCHAR(50)
);

CREATE TABLE Employees (
    EmployeeID INT PRIMARY KEY,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    DepartmentID INT,
    JobTitle VARCHAR(50),
    Salary DECIMAL(10, 2),
    Status VARCHAR(20),
    FOREIGN KEY (DepartmentID) REFERENCES Departments(DepartmentID)
);

CREATE TABLE EmployeePerformance (
    EmployeeID INT,
    DepartmentID INT,
    PerformanceScore INT,
    FOREIGN KEY (EmployeeID) REFERENCES Employees(EmployeeID),
    FOREIGN KEY (DepartmentID) REFERENCES Departments(DepartmentID)
);

CREATE VIEW Managers AS SELECT * FROM Employees WHERE Employees.JobTitle LIKE '%Manager';
CREATE VIEW MainDepartment AS SELECT * FROM Departments WHERE Departments.DepartmentName = 'Main';
CREATE VIEW Outstanding AS SELECT * FROM EmployeePerformance WHERE EmployeePerformance.PerformanceScore > 90;

CREATE FUNCTION getEmployeesUnion(id INT) AS (
  (
    SELECT EmployeeID, FirstName, LastName, DepartmentID, JobTitle, Salary, Status
    FROM Employees
    WHERE DepartmentID = id AND Status = 'Active'
    UNION
    SELECT EmployeeID, FirstName, LastName, DepartmentID, JobTitle, Salary, Status
    FROM Employees
    WHERE FirstName = 'Kostas'
  )
);

CREATE VIEW EmployeePerDepartment AS
SELECT 
    getEmployeesUnion(DepartmentID)
FROM
    Departments
GROUP BY
    DepartmentID;

CREATE VIEW getFirstDepartmentEmployees AS
SELECT 
    getEmployeesUnion(DepartmentID)
FROM 
    Departments    
WHERE
    DepartmentID = 1;