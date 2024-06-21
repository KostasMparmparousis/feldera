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

CREATE FUNCTION getUserByDepartment(id INT) AS (
  (
    SELECT e.EmployeeID, e.FirstName, e.LastName, d.DepartmentName
    FROM Employees e
    JOIN Departments d ON e.DepartmentID = d.DepartmentID
    WHERE d.DepartmentID = id AND e.Status = 'Active'
  )
);

CREATE VIEW EmployeePerDepartment AS
SELECT 
    getUserByDepartment(DepartmentID)
FROM 
    Departments
GROUP BY 
    DepartmentID;

CREATE VIEW getFirstDepartmentEmployees AS
SELECT 
    getUserByDepartment(DepartmentID)
FROM 
    Departments
WHERE
    DepartmentID = 1;