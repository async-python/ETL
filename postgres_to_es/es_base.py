GET_FILMS_BY_ID = '''
SELECT
    fw.id, fw.rating, fw.type, fw.certificate,
    ARRAY_AGG(DISTINCT genre.name ) AS genres,
    fw.title, fw.age_limit, fw.file_path, fw.created_at,
    ARRAY_AGG(CONCAT(person.first_name, person.middle_name, person.last_name)) FILTER (WHERE person_fw.role = 'director') AS directors,
    ARRAY_AGG(CONCAT(person.first_name, person.middle_name, person.last_name)) FILTER (WHERE person_fw.role = 'actor') AS actors,
    ARRAY_AGG(CONCAT(person.first_name, person.middle_name, person.last_name)) FILTER (WHERE person_fw.role = 'writer') AS writers,
    fw.updated_at
FROM film_work AS fw
LEFT OUTER JOIN person_film_work AS person_fw ON fw.id = person_fw.film_work_id
LEFT OUTER JOIN person as person ON person_fw.person_id = person.id
LEFT OUTER JOIN genre_film_work AS genre_fw ON fw.id = genre_fw.film_work_id
LEFT OUTER JOIN genre AS genre ON genre_fw.genre_id = genre.id
WHERE fw.id = "00000cd1-79dc-45c0-abb6-83a25e181a24"
GROUP BY fw.id;
'''