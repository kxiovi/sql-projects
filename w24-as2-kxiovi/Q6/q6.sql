SELECT books.book_id, books.title, 
COUNT(borrowings.book_id) AS bcount
FROM books 
JOIN borrowings ON
books.book_id = borrowings.book_id
LEFT JOIN waitlists ON
borrowings.book_id = waitlists.book_id
WHERE pyear <= 2015
GROUP BY books.book_id, books.title
HAVING bcount > 1
AND bcount >= 2*(COUNT(waitlists.book_id))
;

