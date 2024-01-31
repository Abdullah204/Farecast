-- 1. All trip info(location,tip amount,etc) for the 20 highest trip distances. 
SELECT vendor , pickup_datetime , dropoff_datetime , 
store_and_fwd , L3.old_value asrate_type , pu_location , do_location ,
passenger_count , trip_distance , fare_amount ,
extra , mta_tax , tip_amount , tolls_amount , ehail_fee ,
improvement_surcharge , total_amount , L2.old_value as payment_type , 
L1.old_value as trip_type , congestion_surcharge , week_number , date_range ,
pu_latitude , pu_longitude , do_latitude , do_longitude , 
trip_duration , is_morning_trip , is_weekend_trip 
FROM green_taxi_8_18 T
INNER JOIN lookup_green_taxi_8_18 L1
ON T.trip_type = L1.new_value AND L1.feature_name = 'trip_type'
INNER JOIN lookup_green_taxi_8_18 L2
ON T.payment_type = L2.new_value AND L2.feature_name = 'payment_type'
INNER JOIN lookup_green_taxi_8_18 L3
ON T.rate_type = L3.new_value AND L3.feature_name = 'rate_type'
ORDER BY trip_distance DESC
FETCH FIRST 20 ROWS ONLY 


-- 2. What is the average fare amount per payment type. 
SELECT AVG(fare_amount)AS average_fare_amount , L.old_value as payment_type
FROM green_taxi_8_18 T 
INNER JOIN lookup_green_taxi_8_18 L 
ON T.payment_type = L.new_value AND L.feature_name = 'payment_type'
GROUP BY L.old_value 

-- 3. On average, which city tips the most. 
SELECT pu_location, AVG(tip_amount) 
AS average_tip_amount 
FROM green_taxi_8_18 
GROUP BY pu_location 
ORDER BY average_tip_amount DESC LIMIT 1;

-- 4. On average, which city tips the least. 
SELECT pu_location, AVG(tip_amount) 
AS average_tip_amount 
FROM green_taxi_8_18 
GROUP BY pu_location 
ORDER BY average_tip_amount LIMIT 1;

-- 5. What is the most frequent destination on the weekend. 
SELECT do_location, COUNT(*) AS frequency
FROM green_taxi_8_18 where is_weekend_trip = 1
GROUP BY do_location 
ORDER BY frequency DESC LIMIT 1;
-- 6. On average, which trip type travels longer distances. 
SELECT L.old_value as trip_type , AVG(trip_distance) as avg_distance 
FROM green_taxi_8_18 T INNER JOIN lookup_green_taxi_8_18 L
ON T.trip_type = L.new_value AND L.feature_name = 'trip_type'
GROUP BY L.old_value
ORDER BY avg_distance DESC LIMIT 2;
-- I SET THE LIMIT TO 2 TO SEE WHATS AFTER UKNOWN
-- 7. between 4pm and 6pm what is the average fare amount. 
SELECT AVG(fare_amount) as average_fare_amount
FROM green_taxi_8_18 
WHERE EXTRACT(HOUR FROM dropoff_datetime)  
BETWEEN 16 AND 18 AND EXTRACT(HOUR FROM pickup_datetime)  
BETWEEN 16 AND 18;
