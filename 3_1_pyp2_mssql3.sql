/* -------------------------------------------------- */
/* AUTHOR NAME: MACIEJ TOMASZEWSKI                    */
/* CREATE DATE: 05.03.2024                            */
/* DESCRIPTION:                                       */
/*  MY SECOND PYTHON & SQL PROJECT. THIS SCRIPT IS    */
/*  USED TO PREVIEW SAMPLE, FICTIOUS DATA CREATED     */
/*  FOR THE PURPOSE OF THE PROJECT. THIS IS THE       */
/*  THIRD OF 3 MS SQL SCRIPTS.                        */

/*                  SQL SCRIPT 3/3                    */
/* -------------------------------------------------- */

-- previewing data
SELECT * FROM MT_PythonSQL_Project2.dbo.HR;
SELECT * FROM MT_PythonSQL_Project2.dbo.HR_addr;

-- queries (created in Python separately) used to select data for ETL
SELECT
	h.fname
	,h.lname
	,h.gender
	,h.date_of_birth
	,ha.personal_id
	,ha.id_card_number
	,ha.country
	,ha.city
FROM MT_PythonSQL_Project2.dbo.HR AS h
JOIN MT_PythonSQL_Project2.dbo.HR_addr AS ha
	ON ha.hr_id = h.hr_id
;
