-- A loan reported as PAID_OFF must have no unpaid instalments, and a loan
-- with unpaid instalments must not be reported as PAID_OFF.
--
-- Status and balance are written by different source systems (core lending
-- vs the LMS) and arrive as independent CDC streams. They can therefore
-- disagree — and when they do, every NPL ratio in the gold layer is wrong,
-- because closed loans are excluded from the denominator.

select
    loan_id,
    loan_status,
    installments_unpaid,
    outstanding_principal,
    'PAID_OFF loan still has unpaid instalments' as failure_reason
from {{ ref('fct_loan') }}
where loan_status = 'PAID_OFF'
  and installments_unpaid > 0

union all

select
    loan_id,
    loan_status,
    installments_unpaid,
    outstanding_principal,
    'Fully paid loan not marked PAID_OFF' as failure_reason
from {{ ref('fct_loan') }}
where installments_unpaid = 0
  and loan_status <> 'PAID_OFF'
