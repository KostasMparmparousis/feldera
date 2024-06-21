CREATE TABLE Person
(
    name    VARCHAR NOT NULL,
    age     INT64,
    present BOOLEAN
);

INSERT INTO Person (name, age, present)
VALUES
    ('Alice', 25, TRUE),
    ('Bob', 30, FALSE),
    ('Charlie', 28, TRUE),
    ('David', 25, FALSE),
    ('Emma', 30, TRUE),
    ('Frank', 28, FALSE);

CREATE FUNCTION fetchBasedOnAge(userAge INT64) AS (
  (SELECT * FROM Person WHERE Person.age = userAge AND Person.present = true)
);

CREATE FUNCTION fetchBasedOnAgeAndName(userAge INT64, userName VARCHAR) AS (
  (SELECT * FROM Person WHERE Person.age = userAge AND Person.name = userName)
);

CREATE FUNCTION tempName(userAge INT64) RETURNS VARCHAR AS (
  (SELECT name FROM Person WHERE Person.age = userAge)
);

CREATE FUNCTION tempNameAlt(userAge INT64, userName VARCHAR) AS (
  (SELECT age, present FROM Person WHERE Person.age = userAge AND Person.name = userName)
);

-- CREATE VIEW PersonAgeCounts AS SELECT fetchBasedOnAge(age) FROM Person GROUP BY age;

-- CREATE VIEW PersonNamedAgeCounts AS SELECT fetchBasedOnAgeAndName(age, name) FROM Person GROUP BY age, name;

CREATE VIEW tempViewName AS SELECT tempName(age) FROM Person GROUP BY age;

CREATE VIEW tempViewNameAlt AS SELECT tempNameAlt(age, name) FROM Person GROUP BY age, name;

CREATE VIEW YoungAdults AS SELECT fetchBasedOnAge(age) FROM Person WHERE age=18;

CREATE VIEW YoungAdultsNamedKostas AS SELECT fetchBasedOnAge(age) FROM Person WHERE age=18 AND name='Kostas';