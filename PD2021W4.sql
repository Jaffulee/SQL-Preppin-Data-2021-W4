--Written in Snowflake
--Temporary view for unpivoted table
WITH TablePivot AS (
                    SELECT *
                    -- Union in sub-query to alias
                    FROM    (SELECT *
                                    ,'York' AS store
                            FROM PD2021_WK03_YORK 
                            UNION 
                            SELECT *
                                    ,'Leeds' AS store
                            FROM PD2021_WK03_LEEDS 
                            UNION 
                            SELECT *
                                    ,'London' AS store
                            FROM PD2021_WK03_LONDON
                            UNION
                            SELECT *
                                    ,'Birmingham' AS store
                            FROM PD2021_WK03_BIRMINGHAM
                            UNION
                            SELECT *
                                    ,'Manchester' AS store
                            FROM PD2021_WK03_MANCHESTER
                            ) AS w32021
                    UNPIVOT(products_sold FOR age_product IN ("New_-_Saddles", "New_-_Mudguards","New_-_Wheels","New_-_Bags","Existing_-_Saddles","Existing_-_Mudguards","Existing_-_Wheels","Existing_-_Bags") )
                    )

--Pre-Grouped Table
, TableSplitWithQuarter AS     (
                    SELECT QUARTER(p."Date") AS QUARTER
                            ,p.store
                            ,SPLIT_PART(p.age_product,'_-_',1) AS customer_type
                            ,SPLIT_PART(p.age_product,'_-_',2) AS product
                            ,to_decimal(p.products_sold) AS products_sold
                            FROM TablePivot as p
                    )
--Grouped Table
,GroupedTable AS                (
                    SELECT t."QUARTER"
                                ,t.STORE
                                ,sum(t.PRODUCTS_SOLD) as products_sold
                                -- ,target.Target
                                -- ,sum(t.PRODUCTS_SOLD)-target.Target as variance_to_targert
                                

                                FROM TableSplitWithQuarter as t
                                -- INNER JOIN PD2021_WK04_TARGETS as target
                                -- ON t."QUARTER" = target."QUARTER"
                                GROUP BY t.STORE, t."QUARTER"
                                )

--Join tables
,tablejoin AS                   (
                    SELECT t."QUARTER"
                                ,t.STORE
                                ,t.PRODUCTS_SOLD
                                ,tar."Target" as target
                                ,t.PRODUCTS_SOLD-tar."Target" AS variance_to_target
                                FROM GroupedTable as t
                                INNER JOIN PD2021_WK04_TARGETS as tar
                                ON t."QUARTER" = tar."Quarter" AND t.store = tar."Store"
                                )

--Final Ranking
SELECT  rank() OVER (PARTITION BY v."QUARTER" ORDER BY v.variance_to_target DESC) as "RANK"
        ,*

FROM tablejoin as v


;
