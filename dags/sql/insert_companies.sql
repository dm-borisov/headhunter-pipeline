INSERT INTO companies
SELECT DISTINCT ct.*
  FROM companies_{{ ds_nodash }} ct
       LEFT JOIN companies c ON ct.id = c.id
 WHERE c.id IS NULL;
