# product on-boarding
- create private bq dataset ds 
- service account sa
- grant roles to that sa at project level: none or roles/bigquery.user or roles/bigquery.jobUser
- for that sa grant roles: none or roles/bigquery.dataViewer or roles/bigquery.dataEditor for that private ds (at dataset level)


- create public bq dataset ds
- optionally for that sa grant roles: none or roles/bigquery.dataViewer or roles/bigquery.dataEditor for that public ds (at dataset level) to show that dataset to dataset or authorized access is not needed to return results querying public dataset/view that uses private dataset table behind the scenes


- grant all views from public dataset to access private dataset


- create other service account other-sa
- grant roles to that other-sa at project level: none or roles/bigquery.user or roles/bigquery.jobUser
- for that other-sa grant roles: none or roles/bigquery.dataViewer or roles/bigquery.dataEditor for that public ds (at dataset level). Note other-sa does not roles assigned at public ds level.


## ddl on-boarding
- create table in private ds and insert sample data
- create view in public ds that queries private ds table
- grant public ds view to access private ds table
- create vm GCE with sa email with startup script running queries:
    * bq ls private ds
    * bq query insert/bq head/bq select query/bq delete query private ds table
    * bq select query public ds view
- create vm_other_sa GCE with other-sa email with startup script running queries:
    * bq select query public ds view


Results: depending on the roles:


|   project role  /  dataset role       |  bq ls                                            | bq head                                             | bq query select                                                | bq query insert                                               | bq query delete
| ------------------------------------- | ------------------------------------------------- | --------------------------------------------------- | -------------------------------------------------------------- | ------------------------------------------------------------- | --------------- | 
|               - / -                   | Permission bigquery.tables.list denied on dataset | Permission bigquery.tables.getData denied on table  | User does not have bigquery.jobs.create permission in project  |                                                               |                 |
|               - / bigquery.dataViewer | OK                                                | OK                                                  | User does not have bigquery.jobs.create permission in project  |                                                               |                 |
|bigquery.user    / -                   | OK                                                | Permission bigquery.tables.getData denied on table  | User does not have permission to query table                   | User does not have permission to query table                  |                 |
|bigquery.user    / bigquery.dataViewer | OK                                                | OK                                                  | OK                                                             | Permission bigquery.tables.updateData denied on table         |                 |
|bigquery.jobUser / -                   | Permission bigquery.tables.list denied on dataset | Permission bigquery.tables.getData denied on table  | User does not have permission to query table                   |                                                               |                 |
|bigquery.jobUser / bigquery.dataViewer | OK                                                | OK                                                  | OK                                                             | Permission bigquery.tables.updateData denied on table         |                 |
|bigquery.user    / bigquery.dataEditor | OK                                                | OK                                                  | OK                                                             | OK                                                            |                 |
|               - / bigquery.dataEditor | OK                                                | OK                                                  | User does not have bigquery.jobs.create permission in project  | User does not have bigquery.jobs.create permission in project | OK              |
   