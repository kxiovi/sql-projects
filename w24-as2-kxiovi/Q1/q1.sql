SELECT DISTINCT m.name, m.email 
FROM members m, borrowings b, waitlists w
WHERE b.book_id = w.book_id AND m.email = b.member
	AND m.email = w.member;