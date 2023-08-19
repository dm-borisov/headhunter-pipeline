INSERT INTO companies
SELECT DISTINCT ct.id,
       ct.accredited_it_employer,
       ct.name,
       ct.type,
       ct.area_id,
       ct.site_url,
       ct.alternate_url
  FROM companies_{{ ds_nodash }} ct
       LEFT JOIN companies c ON ct.id = c.id
 WHERE c.id IS NULL;
