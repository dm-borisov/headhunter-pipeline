INSERT INTO skills(skill)
SELECT DISTINCT st.skill s
  FROM skills_{{ ds_nodash }} st
       LEFT JOIN skills s ON s.skill = st.skill
 WHERE s.skill IS NULL;

INSERT INTO vacancies_to_skills
SELECT DISTINCT st.id,
       s.id
  FROM skills_{{ ds_nodash }} st 
       INNER JOIN skills s ON s.skill = st.skill
       LEFT JOIN vacancies_to_skills vts ON st.id = vts.vacancy_id
                                            AND s.id = vts.skill_id      
 WHERE vts.vacancy_id IS NULL
