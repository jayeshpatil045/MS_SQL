--@Author: Jayesh Patil
--@Date: 2024-09-27
--@Last Modified by: Jayesh Patil
--@Title: Perform CURD operation 

--Create Database

CREATE DATABASE curd_operation
--Use DATABASE

use crud_operation
--CRATE TABLE

CREATE TABLE address_book(
	first_name VARCHAR(255),
	last_name VARCHAR(255),
	address VARCHAR(255),
	city VARCHAR(255),
	pin_code INT
	)
--Add values into the Table
INSERT INTO address_book(first_name,last_name,address,city,pin_code)
VALUES('JAYESH','PATIL','NEAR VITTAL MANDIR ','DHULE',425403),
      ('HITESH','PATIL','KAZHI NAGER','SHIRPUR',425406),
	  ('RAJ','BHEHERE','SHIVAJI NAGER','MANDANE',435403);

--Display the details
SELECT * FROM address_book

--Display first_name and city only
SELECT first_name , city FROM address_book

--UPDATE VALUE IN TABLE 
UPDATE address_book
SET first_name = 'Dipkumar',last_name = 'Patil' 
WHERE first_name = 'JAY'

SELECT * FROM address_book

--Delete One ROW From address_book
DELETE address_book
WHERE first_name = 'RAJ'

SELECT * FROM address_book

--Display address_book where first_name is sort by ascending order.
SELECT * FROM address_book ORDER BY 'first_name' ASC;

--Display address_book where first_name is sort by Descending order.
SELECT * FROM address_book ORDER BY 'first_name' DESC;




