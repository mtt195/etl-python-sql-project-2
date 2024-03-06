/* -------------------------------------------------- */
/* AUTHOR NAME: MACIEJ TOMASZEWSKI                    */
/* CREATE DATE: 05.03.2024                            */
/* DESCRIPTION:                                       */
/*  MY SECOND PYTHON & SQL PROJECT. THIS SCRIPT IS    */
/*  USED TO CREATE TABLES WITH SAMPLE DATA FOR	      */
/*  THE PURPOSE OF THE PROJECT - TO PERFORM ETL       */
/*  PROCESS. THIS IS THE SECOND OF 3 MS SQL SCRIPTS.  */

/*                  SQL SCRIPT 2/3                    */
/* -------------------------------------------------- */

-- deleting tables if exist
IF OBJECT_ID('MT_PythonSQL_Project2.dbo.HR') IS NOT NULL
BEGIN
	DROP TABLE MT_PythonSQL_Project2.dbo.HR
END;

IF OBJECT_ID('MT_PythonSQL_Project2.dbo.HR_addr') IS NOT NULL
BEGIN
	DROP TABLE MT_PythonSQL_Project2.dbo.HR_addr
END;

-- creating tables
CREATE TABLE MT_PythonSQL_Project2.dbo.HR
(
	lp int identity(1,1)
	,hr_id int
	,fname varchar(50)
	,lname varchar(50)
	,gender char(1)
	,date_of_birth date
);

CREATE TABLE MT_PythonSQL_Project2.dbo.HR_addr
(
	lp int identity(1,1)
	,hr_id int
	,personal_id int
	,id_card_number char(7)
	,country varchar(50)
	,city varchar(50)
);

-- inserting data
INSERT INTO MT_PythonSQL_Project2.dbo.HR(hr_id, fname, lname, gender, date_of_birth)
VALUES
	(111, 'Jakub', 'Jakubczyk', 'M', '1985-09-21')
	,(222, 'Ziuta', 'Ziuciñska', 'F', '1987-01-21')
	,(333, 'Aleksandra', 'Alowska', 'F', '1980-07-13')
;

INSERT INTO MT_PythonSQL_Project2.dbo.HR_addr(hr_id, personal_id, id_card_number, country, city)
VALUES
	(111, 37109273, 'J733566', 'Poland', 'Cracow')
	,(222, 36224140, 'Z390665', 'Poland', 'Wroclaw')
	,(333, 18156872, 'A139601', 'Poland', 'Cracow')
;

-- previewing data
SELECT * FROM MT_PythonSQL_Project2.dbo.HR;
SELECT * FROM MT_PythonSQL_Project2.dbo.HR_addr;
