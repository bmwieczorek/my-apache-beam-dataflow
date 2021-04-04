SELECT
  *, `${project}.${dataset}.my_sum`(numbers) as sum
FROM
  `${project}.${dataset}.${table}`