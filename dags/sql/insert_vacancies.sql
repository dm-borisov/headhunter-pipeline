INSERT INTO vacancies
SELECT DISTINCT vt.id,
       vt.name,
       vt.area_id,
       vt.employer_id,
       vt.published_at,
       vt.experience_id,
       vt.schedule_id,
       vt.professional_roles,
       vt.alternate_url,
       vt.salary_from,
       vt.salary_to,
       vt.salary_currency,
       vt.salary_gross
  FROM vacancies_{{ ds_nodash }} vt
       LEFT JOIN vacancies v ON v.id = vt.id
 WHERE v.id IS NULL
