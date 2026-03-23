CREATE VIEW book_info AS
SELECT books.book_id, books.title, 
COUNT(DISTINCT(reviews.rid)) AS revcnt, 
COALESCE(AVG(reviews.rating), 0) AS rating,
COALESCE(AVG(CASE WHEN strftime('%Y', reviews.rdate) = '2023' THEN reviews.rating END), 0) AS rating23,
IFNULL(COUNT(DISTINCT(borrowings.bid)) + COUNT(DISTINCT(waitlists.wid)), 0) as reqcnt
FROM books
LEFT JOIN borrowings ON
books.book_id = borrowings.book_id
LEFT JOIN waitlists ON
books.book_id = waitlists.book_id
LEFT JOIN reviews ON
books.book_id = reviews.book_id
GROUP BY books.book_id, books.title
;

SELECT * 
FROM book_info
;
