-- GOLD LAYER FOR BUSINESS USECASE
-- Answer the business cases

SELECT 
    ANY_VALUE(p.VehicleReg) AS VehicleReg,
    ANY_VALUE(TIMESTAMP_MILLIS(p.EntryDate)) AS EntryDate,
    ANY_VALUE(g.GateName) AS GateName,
    g.KrugerCampName,
    COUNT(g.KrugerCampName) AS Normal_days_count_visited
FROM
    `project-a2ce378b-71f9-4087-95b.silver_dataset.park_entries` p
        INNER JOIN
    `project-a2ce378b-71f9-4087-95b.silver_dataset.gate_codes` g ON p.EntryGateID = g.GateID
GROUP BY g.KrugerCampName
ORDER BY Normal_days_count_visited DESC
;

 -- 1.2 During september Letaba still dominate then, Crocodile bridge and shimuwini
SELECT 
    ANY_VALUE(p.VehicleReg) AS VehicleReg,
    ANY_VALUE(TIMESTAMP_MILLIS(p.EntryDate)) AS EntryDate,
    ANY_VALUE(g.GateName) AS GateName,
    g.KrugerCampName,
    COUNT(g.KrugerCampName) AS Sept_count_visited
FROM
    `project-a2ce378b-71f9-4087-95b.silver_dataset.park_entries` p
        INNER JOIN
    `project-a2ce378b-71f9-4087-95b.silver_dataset.gate_codes` g ON p.EntryGateID = g.GateID
WHERE
    EXTRACT(MONTH FROM TIMESTAMP_MILLIS(p.EntryDate)) = 9
GROUP BY g.KrugerCampName
ORDER BY Sept_count_visited DESC
;
-- 2. Between Visitors with wild card and visitors without wild-cards who gets to visit more in the park ?
-- 2.1 Visitors without wildcards visit more than those with wildcards
SELECT 
    COUNT(WildCardMember) AS Wildcard_or_NoWildcard,
    WildCardMember
FROM
    `project-a2ce378b-71f9-4087-95b.silver_dataset.visitors`
GROUP BY WildCardMember
ORDER BY WildCardMember DESC
;
-- 3. Which payment type do visitors like to pay with
-- 3.2 visitors love to pay with cash and with credit card
SELECT 
    ANY_VALUE(VisitorID) AS VisitorID,
    ANY_VALUE(AccID) AS AccID,
    PaymentType,
    COUNT(PaymentType) AS Number_of_PaymentType
FROM
    `project-a2ce378b-71f9-4087-95b.silver_dataset.billing`
GROUP BY PaymentType
ORDER BY Number_of_PaymentType DESC
;

-- 4. Which month other than September do people also like visiting the Park ?
-- 4.1 August is where people visit kruger more often

SELECT 
    ANY_VALUE(VisitorID) AS VisitorID,
    ANY_VALUE(PaymentType) AS PaymentType,
    FORMAT_TIMESTAMP('%B', TIMESTAMP_MILLIS(VisitDate)) AS month_name,
    COUNT(FORMAT_TIMESTAMP('%B', TIMESTAMP_MILLIS(VisitDate))) AS number_of_month
FROM
    `project-a2ce378b-71f9-4087-95b.silver_dataset.billing`
GROUP BY month_name
ORDER BY number_of_month DESC
;
