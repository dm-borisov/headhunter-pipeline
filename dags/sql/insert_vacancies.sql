INSERT INTO vacancies
SELECT DISTINCT vt.*
  FROM vacancies_{{ ds_nodash }} vt
       LEFT JOIN vacancies v ON v.id = vt.id
 WHERE v.id IS NULL
