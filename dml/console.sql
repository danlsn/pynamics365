USE MIPCRM_Sandbox;
GO
-- Create view of load_accounts table
-- Columns: ['accountid', 'name', '_ownerid_value', '_parrentaccountid_value', '_primarycontactid_value', 'address*',
-- 'description', 'emailaddress*', 'mip_*', 'stageid', 'statuscode', 'telephone1', 'telephone2', 'telephone3', 'websiteurl']

DROP VIEW IF EXISTS ds_accounts;
GO
CREATE VIEW ds_accounts
AS
SELECT la.accountid,
       la.name,
       la._ownerid_value,
       lsu.fullname,
       la._parentaccountid_value,
       la._primarycontactid_value,
       la.address1_primarycontactname,
       la.address1_composite,
       la.address1_city,
       la.address1_county,
       la.address1_country,
       la.address1_postalcode,
       la.address2_primarycontactname,
       la.address2_composite,
       la.address2_city,
       la.address2_country,
       la.address2_postalcode,
       la.description,
       la.emailaddress1,
       la.emailaddress2,
       la.emailaddress3,
       la.stageid,
       la.statuscode,
       la.telephone1,
       la.telephone2,
       la.telephone3,
       la.websiteurl
FROM load_accounts AS la
         JOIN load_systemusers AS lsu
              ON la._ownerid_value = lsu.systemuserid
WHERE lsu.isdisabled = '0'
  AND la.statuscode = '1';

-- Create view showing count of contacts by state
-- Columns: ['state', 'count']

DROP VIEW IF EXISTS ds_contacts_by_state;
GO
CREATE VIEW ds_contacts_by_state
AS
SELECT address1_stateorprovince AS state,
       COUNT(*) AS count
FROM load_contacts
GROUP BY address1_stateorprovince;



delete
from activity_histories
where;
