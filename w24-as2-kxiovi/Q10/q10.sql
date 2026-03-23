SELECT DISTINCT members.email
FROM members
JOIN borrowings ON
members.email = borrowings.member
JOIN book_info ON
borrowings.book_id = book_info.book_id
WHERE 
book_info.rating > 3.5
AND book_info.reqcnt > (
SELECT AVG(book_info.reqcnt)
FROM book_info
)
;