select rider_id, COUNT(rider_id) as "Rides Booked", sum(case when charged_to_rider = 0 then 0 else 1 end) as "# of rides paid", SUM(charged_to_rider) as "Total Cost", airport
from rides
where charged_to_rider is not null
group by rider_id
