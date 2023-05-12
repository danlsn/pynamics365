update [itknocks_migration].contacts
set middle_name = null
where middle_name = '';

select *
from [itknocks_migration].contacts
where email like 'prashanth.pejavar.rao@sg.pwc.com';
