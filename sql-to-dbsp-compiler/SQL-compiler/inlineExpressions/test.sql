-- define Person table
CREATE TABLE Person
(
    name    VARCHAR NOT NULL,
    age     INT,
    present BOOLEAN
);
CREATE VIEW Adult AS SELECT Person.name FROM Person WHERE Person.age > 18
UNION
SELECT Person.name FROM Person WHERE Person.age <10;

CREATE FUNCTION AddFourAndDivide(x DECIMAL, y DECIMAL)
RETURNS BOOLEAN
AS ((x+4)/y);
