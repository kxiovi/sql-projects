-- SELECT bid, member, end_date 
-- FROM borrowings
-- , books
-- , members
-- where borrowings.book_id = books.book_id
-- and borrowings.member = members.email
-- and (books.author like '%John%'
-- or books.author like '%Marry%'
-- )
-- and members.faculty = 'CS'
-- UNION
SELECT bid, member, end_date
FROM borrowings
WHERE book_id IN (
SELECT DISTINCT book_id
FROM borrowings
, members
WHERE borrowings.member = members.email
AND members.faculty = 'CS'
)
AND book_id IN (
SELECT book_id from books
WHERE (books.author LIKE '%John%'
OR books.author LIKE '%Marry%'
)
);

