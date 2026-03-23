SELECT book_id, title, pyear
FROM (
SELECT borrowings.book_id, books.title, books.pyear,
DENSE_RANK() OVER (
ORDER BY COUNT(borrowings.bid) DESC
)
AS brank
FROM books
LEFT JOIN borrowings ON 
books.book_id = borrowings.book_id
GROUP BY borrowings.book_id, books.title, books.pyear
) 
WHERE brank < 4
;