DROP TABLE people IF EXISTS;
DROP TABLE integration_people IF EXISTS;

CREATE TABLE people  (
    person_id BIGINT IDENTITY NOT NULL PRIMARY KEY,
    first_name VARCHAR(20),
    last_name VARCHAR(20)
);

CREATE TABLE integration_people  (
    person_id BIGINT IDENTITY NOT NULL PRIMARY KEY,
    first_name VARCHAR(30) NOT NULL,
    last_name VARCHAR(30) NOT NULL,
    email_address VARCHAR(40) NOT NULL UNIQUE,
    date_of_birth DATE NOT NULL
);