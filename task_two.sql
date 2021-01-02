
SELECT department,
	employee,
	salary
FROM (
	SELECT 	d.name department,
		e.name employee,
		salary, 
		DENSE_RANK() OVER (PARTITION BY d.id ORDER BY salary DESC ) salary_rank
	FROM 	Employee e JOIN 
		    Department d ON e.departmentid=d.id
)a  
WHERE salary_rank <=3 
ORDER BY department,salary
