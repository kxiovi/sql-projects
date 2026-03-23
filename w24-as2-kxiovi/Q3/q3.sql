SELECT bid, member
FROM borrowings
WHERE julianday(end_date) - julianday(start_date) > 14
AND 
(SELECT priority FROM waitlists
WHERE priority < 5
);