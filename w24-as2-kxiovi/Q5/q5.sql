-- SELECT reviews.book_id, books.title, AVG(reviews.rating) AS raverage
-- FROM books
-- JOIN reviews ON 
-- books.book_id = reviews.book_id
-- WHERE reviews.book_id IN (
-- SELECT reviews.book_id
-- FROM reviews
-- GROUP BY reviews.book_id
-- HAVING COUNT(reviews.book_id) >= 2
-- LIMIT 3
-- )
-- GROUP BY reviews.book_id, books.title
-- ORDER BY raverage DESC
-- ;

SELECT reviews.book_id, books.title, AVG(reviews.rating) AS raverage
FROM reviews
JOIN books ON 
reviews.book_id = books.book_id
GROUP BY reviews.book_id, books.title
HAVING COUNT(reviews.rid) >= 2
ORDER BY raverage DESC
LIMIT 3
;