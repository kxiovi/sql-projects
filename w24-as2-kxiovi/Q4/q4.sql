SELECT books.book_id, books.title, books.author, 
COUNT(borrowings.book_id) as bcount, 
MAX(borrowings.start_date) as rdate
FROM books
LEFT JOIN borrowings ON 
books.book_id = borrowings.book_id
WHERE pyear > 2001
GROUP BY books.book_id, books.title, books.author
;
