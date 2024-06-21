CREATE TABLE Person
(
    name    VARCHAR NOT NULL,
    age     INT64,
    present BOOLEAN
);

INSERT INTO Person (name, age, present)
VALUES
    ('Kostas', 24, TRUE),
    ('Spiros', 24, TRUE),
    ('Antonis', 24, TRUE),
    ('Nikos', 25, TRUE),
    ('Vaggos', 25, TRUE);

CREATE FUNCTION countUserByAge(userAge INT64) RETURNS INT64 AS (
  (SELECT COUNT(1) FROM Person WHERE Person.age = userAge AND Person.present = true)
);

CREATE FUNCTION countUserByAgeAndName(userAge INT64, userName VARCHAR) RETURNS INT64 AS (
  (SELECT COUNT(1) FROM Person WHERE Person.age = userAge AND Person.name = userName)
);

CREATE FUNCTION getUserNameByAge(userAge INT64) RETURNS VARCHAR AS (
  (SELECT name FROM Person WHERE Person.age = userAge AND Person.present = true)
);

CREATE FUNCTION getActiveEmployees() AS (
  (SELECT name, age FROM Person WHERE Person.present = true)
);

CREATE FUNCTION getActiveEmployeesAlt() AS (
  (SELECT name FROM Person WHERE Person.present = true)
);

CREATE VIEW Adults AS SELECT * FROM Person WHERE Person.age > 18;

CREATE VIEW PersonAgeCounts AS SELECT age, present, countUserByAge(age) AS present_count FROM Person GROUP BY age;

CREATE VIEW PersonNamedAgeCounts AS SELECT age, name, countUserByAgeAndName(age, name) FROM Person GROUP BY age, name;

CREATE VIEW PersonAgeName AS SELECT age, present, getUserNameByAge(age) FROM Person GROUP BY age;

CREATE VIEW PersonAgeNameAlt AS SELECT getUserNameByAge(age) FROM Person GROUP BY age;

CREATE VIEW YoungAdults AS SELECT age, countUserByAge(age) AS present_count FROM Person WHERE age=24;

CREATE VIEW YoungAdultsNamedKostas AS SELECT age, present, countUserByAgeAndName(age, name) AS present_count FROM Person WHERE age=18 AND name='Kostas';

CREATE VIEW PersonAgeCountsAlias AS SELECT age AS temp_age, present AS test, countUserByAge(age) AS present_count FROM Person GROUP BY age HAVING present_count > 1;

CREATE VIEW ActivePersons AS SELECT getActiveEmployees() FROM Person;

CREATE VIEW ActivePersonsAlt AS SELECT getActiveEmployeesAlt() FROM Person;