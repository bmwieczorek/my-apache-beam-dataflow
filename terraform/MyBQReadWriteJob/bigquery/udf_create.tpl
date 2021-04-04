CREATE OR REPLACE FUNCTION `${project}.${dataset}.my_sum` (arr ANY TYPE) AS (
    (SELECT SUM(x) FROM UNNEST(arr) AS x)
);