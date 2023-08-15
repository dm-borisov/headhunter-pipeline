INSERT INTO vacancies
SELECT DISTINCT vt.*
  FROM vacancies_tmp vt
       LEFT JOIN vacancies v ON v.id = vt.id
 WHERE v.id IS NULL
