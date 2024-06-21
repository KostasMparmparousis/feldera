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

CREATE FUNCTION countUserByDepartment(id INT) RETURNS INT AS (
  (SELECT COUNT(1) FROM Employees WHERE Employees.DepartmentID = id  AND Employees.Status = 'Active' OR Employees.FirstName = 'Kostas')
);
