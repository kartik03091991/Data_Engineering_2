--Custom_SQL_Query

SELECT
    payment_type,
    case
        when Payment_type = 1 then 'Credit card'
        when Payment_type = 2 then 'Cash'
        when Payment_type = 3 then 'No charge'
        when Payment_type = 4 then 'Dispute'
        when Payment_type = 5 then 'Unknown'
        when Payment_type = Null then 'Voided trip'
        end as PaymentType,
    COUNT(*) AS total_trips,
    AVG(passenger_count) AS average_passenger_count,
    AVG(tip_amount) AS average_tip_amount
FROM
    TAXI.GreenTaxi_batch
WHERE
    fare_amount > 0 and 
    passenger_count > 0
GROUP BY
    payment_type
	
	

--Custom_SQL_Query1

SELECT
    VendorID,
    case when vendorID = 1 then 'Creative Mobile Technologies'
    when vendorID = 2 then 'VeriFone Inc'
    End as VendorName ,
    COUNT(*) AS total_trips,
    AVG(fare_amount) AS average_fare_amount,
    SUM(tip_amount) AS total_tip_amount,
    SUM(tolls_amount) AS total_tolls_amount
FROM
    TAXI.GreenTaxi_batch
WHERE
    fare_amount > 0
GROUP BY
    VendorID
ORDER BY
    total_trips DESC
	
	
--Custom_SQL_Query2

SELECT
    MONTH(lpep_pickup_datetime) AS Month,
    COUNT(*) AS total_trips
FROM
    TAXI.GreenTaxi_batch
WHERE DATE(lpep_pickup_datetime) >= '2022-12-01' AND DATE(lpep_pickup_datetime) < '2023-03-01'
GROUP BY
    Month
ORDER BY
    Month
	
	
	
--Custom_SQL_Query3

SELECT
    MONTH(lpep_pickup_datetime) AS pickup_month,
    CASE
        WHEN MONTH(lpep_pickup_datetime) = 12 THEN 'Dec 2022'
        WHEN MONTH(lpep_pickup_datetime) = 1 THEN 'Jan 2023'
        WHEN MONTH(lpep_pickup_datetime) = 2 THEN 'Feb 2023'
        ELSE 'Other'
    END AS pickup_month_name,
    COUNT(*) AS total_trips
FROM
    TAXI.GreenTaxi_batch
WHERE
    DATE(lpep_pickup_datetime) >= '2022-12-01' AND DATE(lpep_pickup_datetime) < '2023-03-01'
GROUP BY
    MONTH(lpep_pickup_datetime), pickup_month_name
ORDER BY
    pickup_month
	

--Custom_SQL_Query4


SELECT 
t2.Zone,
    COUNT(*) AS pickup_count,
    COUNT(*) AS total_trips
FROM TAXI.GreenTaxi_batch t1
INNER JOIN TAXI.taxi_zone_lookup t2
    ON t1.PULocationID = t2.LocationID
GROUP BY 
t1.PULocationID , t2.Zone
ORDER By pickup_count desc
limit 10


--Custom_SQL_Query5

SELECT 
t2.Zone,
    COUNT(*) AS Drop_count,
    COUNT(*) AS total_trips
FROM TAXI.GreenTaxi_batch t1
INNER JOIN TAXI.taxi_zone_lookup t2
    ON t1.DOLocationID = t2.LocationID
GROUP BY 
t1.DOLocationID , t2.Zone
ORDER By Drop_count desc
limit 10