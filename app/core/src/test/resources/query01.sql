select Symbol, Name, Sector, Industry, `Summary Quote`
from Customers
where Industry = 'Oil/Gas Transmission'
union
select Symbol, Name, Sector, Industry, `Summary Quote`
from Customers
where Industry = 'Computer Manufacturing'