INSERT INTO companies_to_industries(company_id, industry_id)
SELECT DISTINCT it.id,
       it.industry_id
  FROM industries_{{ ds_nodash }} it
       LEFT JOIN companies_to_industries cti ON it.id = cti.company_id
                                                AND it.industry_id = cti.industry_id
 WHERE cti.industry_id IS NULL;
