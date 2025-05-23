select * from {{ ref('stg_customers') }} c where length(c.email) > 2*(
    select top 1 length(c.email) from {{ ref('stg_customers') }} c order by length(c.email) desc
)