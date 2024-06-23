CREATE TABLE Person
(
    name    VARCHAR NOT NULL,
    age     INT64 NOT NULL,
    present BOOLEAN
);

INSERT INTO Person (name, age, present)
VALUES
    ('Kostas', 24, TRUE),
    ('Spiros', 24, TRUE),
    ('Antonis', 24, TRUE),
    ('Nikos', 25, TRUE),
    ('Vaggos', 25, TRUE);

CREATE TABLE TempAggregates
(
    metric_value INT64 NOT NULL
);

CREATE TABLE TempAggregatesDouble
(
    metric_value float64 NOT NULL
);

CREATE TABLE TEMP("USERAGE" INT64 NOT NULL);

INSERT INTO TEMP(USERAGE)
SELECT DISTINCT age FROM Person;

INSERT INTO TempAggregates (metric_value)
SELECT COUNT(*)
FROM Person;

INSERT INTO TempAggregates (metric_value)
SELECT COUNT(age)
FROM Person;

INSERT INTO TempAggregates (metric_value)
SELECT MIN(age)
FROM Person;

INSERT INTO TempAggregates (metric_value)
SELECT MAX(age)
FROM Person;

INSERT INTO TempAggregatesDouble (metric_value)
SELECT AVG(age)
FROM Person;

INSERT INTO TempAggregates (metric_value)
SELECT SUM(age)
FROM Person;