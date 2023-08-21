
CREATE OR REPLACE FUNCTION public.debit_and_credit_posting(
	date_posting date)
    RETURNS TABLE(date_debit_and_credit date, posting_max double precision,  posting_min double precision)
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
BEGIN
    RETURN QUERY SELECT
        oper_date, MAX(credit_amount)+MAX(debet_amount), MIN(credit_amount)+MIN(debet_amount)
    FROM
        ds.ft_posting_f
	WHERE oper_date = date_posting
	GROUP BY oper_date;
END;
$BODY$;
