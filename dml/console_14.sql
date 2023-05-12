exec sp_addextendedproperty 'MS_Description', 'Maximum Length: 100 characters.', 'SCHEMA', 'itknocks_migration',
     'TABLE', 'contacts', 'COLUMN', 'salutation'
go

exec sp_addextendedproperty 'MS_Description', 'Maximum Length: 50 characters.', 'SCHEMA', 'itknocks_migration', 'TABLE',
     'contacts', 'COLUMN', 'first_name'
go

alter table itknocks_migration.contacts
    alter column first_name varchar(50) not null
go

exec sp_addextendedproperty 'MS_Description', 'Maximum Length: 50 characters.', 'SCHEMA', 'itknocks_migration', 'TABLE',
     'contacts', 'COLUMN', 'middle_name'
go

exec sp_addextendedproperty 'MS_Description', 'Maximum Length: 50 characters.', 'SCHEMA', 'itknocks_migration', 'TABLE',
     'contacts', 'COLUMN', 'last_name'
go

alter table itknocks_migration.contacts
    alter column last_name varchar(50) not null
go

exec sp_addextendedproperty 'MS_Description', 'Maximum Length: 100 characters.', 'SCHEMA', 'itknocks_migration',
     'TABLE', 'contacts', 'COLUMN', 'job_title'
go

alter table itknocks_migration.contacts
    alter column job_title varchar(100) not null
go

exec sp_updateextendedproperty 'MS_Description',
     'Lookup: This Company Name record must already exist in Microsoft Dynamics 365 or in this source file.', 'SCHEMA',
     'itknocks_migration', 'TABLE', 'contacts', 'COLUMN', 'company_name'
go

exec sp_addextendedproperty 'MS_Description', 'Maximum Length: 50 characters.', 'SCHEMA', 'itknocks_migration', 'TABLE',
     'contacts', 'COLUMN', 'business_phone'
go

alter table itknocks_migration.contacts
    alter column business_phone varchar(50) not null
go

exec sp_addextendedproperty 'MS_Description', 'Maximum Length: 50 characters.', 'SCHEMA', 'itknocks_migration', 'TABLE',
     'contacts', 'COLUMN', 'home_phone'
go

exec sp_addextendedproperty 'MS_Description', 'Maximum Length: 50 characters.', 'SCHEMA', 'itknocks_migration', 'TABLE',
     'contacts', 'COLUMN', 'fax'
go

exec sp_addextendedproperty 'MS_Description', 'Maximum Length: 100 characters.', 'SCHEMA', 'itknocks_migration',
     'TABLE', 'contacts', 'COLUMN', 'email'
go

exec sp_updateextendedproperty 'MS_Description', 'Address 1: Address Type must be selected from the drop-down list.
(''Bill To'', ''Ship To'', ''Primary'', ''Other'')', 'SCHEMA', 'itknocks_migration', 'TABLE', 'contacts', 'COLUMN',
     'address_1_address_type'
go

exec sp_addextendedproperty 'MS_Description', 'Maximum Length: 200 characters.', 'SCHEMA', 'itknocks_migration',
     'TABLE', 'contacts', 'COLUMN', 'address_1_name'
go

exec sp_addextendedproperty 'MS_Description', 'Maximum Length: 250 characters.', 'SCHEMA', 'itknocks_migration',
     'TABLE', 'contacts', 'COLUMN', 'address_1_street_1'
go

exec sp_addextendedproperty 'MS_Description', 'Maximum Length: 250 characters.', 'SCHEMA', 'itknocks_migration',
     'TABLE', 'contacts', 'COLUMN', 'address_1_street_2'
go

exec sp_addextendedproperty 'MS_Description', 'Maximum Length: 250 characters.', 'SCHEMA', 'itknocks_migration',
     'TABLE', 'contacts', 'COLUMN', 'address_1_street_3'
go

exec sp_addextendedproperty 'MS_Description', 'Maximum Length: 80 characters.', 'SCHEMA', 'itknocks_migration', 'TABLE',
     'contacts', 'COLUMN', 'address_1_city'
go

exec sp_addextendedproperty 'MS_Description', 'Maximum Length: 50 characters.', 'SCHEMA', 'itknocks_migration', 'TABLE',
     'contacts', 'COLUMN', 'address_1_state_province'
go

exec sp_addextendedproperty 'MS_Description', 'Maximum Length: 20 characters.', 'SCHEMA', 'itknocks_migration', 'TABLE',
     'contacts', 'COLUMN', 'address_1_zip_postal_code'
go

exec sp_addextendedproperty 'MS_Description', 'Maximum Length: 80 characters.', 'SCHEMA', 'itknocks_migration', 'TABLE',
     'contacts', 'COLUMN', 'address_1_country_region'
go

exec sp_addextendedproperty 'MS_Description', 'Maximum Length: 50 characters.', 'SCHEMA', 'itknocks_migration', 'TABLE',
     'contacts', 'COLUMN', 'address_1_phone'
go

exec sp_updateextendedproperty 'MS_Description', 'Address 1: Freight Terms must be selected from the drop-down list.
(''FOB'', ''No Charge'')', 'SCHEMA', 'itknocks_migration', 'TABLE', 'contacts', 'COLUMN', 'address_1_freight_terms'
go

exec sp_updateextendedproperty 'MS_Description', 'Address 1: Shipping Method must be selected from the drop-down list.
(''Airborne'', ''DHL'', ''FedEx'', ''UPS'', ''Postal Mail'', ''Full Load'', ''Will Call'')', 'SCHEMA',
     'itknocks_migration', 'TABLE', 'contacts', 'COLUMN', 'address_1_shipping_method'
go

exec sp_addextendedproperty 'MS_Description', 'Maximum Length: 2000 characters.', 'SCHEMA', 'itknocks_migration',
     'TABLE', 'contacts', 'COLUMN', 'description'
go

alter table itknocks_migration.contacts
    alter column description varchar(2000) null
go

exec sp_addextendedproperty 'MS_Description', 'Maximum Length: 100 characters.', 'SCHEMA', 'itknocks_migration',
     'TABLE', 'contacts', 'COLUMN', 'department'
go

