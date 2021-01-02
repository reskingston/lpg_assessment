
SELECT request_at "Day", 
	COALESCE(ROUND(SUM( CASE WHEN status <> 'completed'  AND CLIENT.banned='No' THEN 1 ELSE 0 END ) / (SUM(CASE WHEN client.banned='No' THEN 1 END))::float,2),0) cancellation_rate  
FROM 	dp_dw_playpen_analyst.trips t LEFT OUTER JOIN  
	dp_dw_playpen_analyst.Users client ON client.users_id=client_id LEFT OUTER JOIN  
	dp_dw_playpen_analyst.Users drivers ON drivers.users_id=driver_id 
	
GROUP BY request_at
ORDER BY request_at
