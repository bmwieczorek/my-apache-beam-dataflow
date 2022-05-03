SELECT
  *, n[SAFE_OFFSET(0)] as n0, n[SAFE_OFFSET(1)] as n1
FROM
  (SELECT
    *, `${project}.${dataset}.my_even_numbers`(numbers) as n
   FROM
    `${project}.${dataset}.${table}`
  )
