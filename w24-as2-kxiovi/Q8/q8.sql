WITH penalty_cte(email, tot_pen, paid_pen, tot_paid) AS (
SELECT members.email,
COUNT(penalties.pid) AS tot_pen,
SUM(CASE WHEN penalties.paid_amount >= penalties.amount THEN 1 ELSE 0 END) AS paid_pen, 
SUM(CASE WHEN penalties.paid_amount >= penalties.amount THEN penalties.paid_amount ELSE 0 END) AS tot_paid
FROM members
LEFT JOIN borrowings ON
members.email = borrowings.member
JOIN penalties ON
borrowings.bid = penalties.bid
GROUP BY members.email
)
SELECT members.email as members, cte.tot_pen, cte.paid_pen, cte.tot_paid
FROM members
JOIN penalty_cte cte ON
members.email = cte.email
;
