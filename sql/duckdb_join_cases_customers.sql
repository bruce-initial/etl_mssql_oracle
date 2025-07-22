-- Custom query with filters for CASES table
-- Used when custom_query is specified in configuration
SELECT 
    CASE_ID,
    CUSTOMER_ID,
    CASE_TITLE,
    CASE_DESCRIPTION,
    STATUS,
    PRIORITY,
    CREATED_DATE,
    MODIFIED_DATE,
    ASSIGNED_TO
FROM {schema}.CASES
WHERE STATUS IN ('OPEN', 'IN_PROGRESS', 'PENDING')
ORDER BY CREATED_DATE DESC