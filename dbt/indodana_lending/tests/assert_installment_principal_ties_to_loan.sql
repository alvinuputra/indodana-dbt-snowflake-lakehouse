-- The instalment schedule must amortise exactly the disbursed principal.
--
-- This is the test that catches silent money bugs: a rounding error in the
-- amortisation, a schedule generated against a stale principal, or a loan
-- whose amount was revised after the schedule was cut. Any of those would
-- pass every uniqueness and not-null test while quietly misstating the
-- book. Tolerance is IDR 1 to absorb nothing more than float noise.

select
    f.loan_id,
    f.principal_amount,
    sum(i.principal_due)                                as scheduled_principal,
    f.principal_amount - sum(i.principal_due)           as variance
from {{ ref('fct_loan') }} f
join {{ ref('fct_installment') }} i on i.loan_id = f.loan_id
group by f.loan_id, f.principal_amount
having abs(f.principal_amount - sum(i.principal_due)) > 1
