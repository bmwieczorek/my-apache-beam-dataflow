locals {
  ts = formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())
}

resource "google_service_account" "sa" {
  project      = var.project
  account_id   = "${var.project}-${var.owner}-new-sa"
  display_name = "${var.owner} new sa"
}

resource "google_project_iam_member" "add_bigquery_role_to_sa_for_project" {
  project = var.project
  role    = "roles/bigquery.user"
  member  = "serviceAccount:${google_service_account.sa.email}"
}

resource "null_resource" "print-sa-roles-1" {
  depends_on = [ google_project_iam_member.add_bigquery_role_to_sa_for_project ]
  triggers = {
    always_run = local.ts
  }

  provisioner "local-exec" {
    command = <<-EOF
      gcloud projects get-iam-policy ${var.project} \
      --filter="bindings.members:${google_service_account.sa.email}" \
      --flatten="bindings[].members" \
      --format='table(bindings.role)'
    EOF
  }
}

resource "google_project_iam_member" "add_bigtable_role_to_sa_for_project" {
  depends_on = [ google_project_iam_member.add_bigquery_role_to_sa_for_project ]
  project = var.project
  role    = "roles/bigtable.user"
  member  = "serviceAccount:${google_service_account.sa.email}"
}

resource "null_resource" "print-sa-roles-2" {
  depends_on = [ google_project_iam_member.add_bigtable_role_to_sa_for_project ]

  triggers = {
    always_run = local.ts
  }

  provisioner "local-exec" {
    command = <<-EOF
      gcloud projects get-iam-policy ${var.project} \
      --filter="bindings.members:${google_service_account.sa.email}" \
      --flatten="bindings[].members" \
      --format='table(bindings.role)'
    EOF
  }
}

#
#terraform apply -auto-approve
#google_service_account.sa: Creating...
#google_service_account.sa: Creation complete after 1s
#google_project_iam_member.add_bigquery_role_to_sa_for_project: Creating...
#google_project_iam_member.add_bigquery_role_to_sa_for_project: Creation complete after 9s [id=.../roles/bigquery.user/serviceAccount:...]
#google_project_iam_member.add_bigtable_role_to_sa_for_project: Creating...
#null_resource.print-sa-roles-1: Creating...
#null_resource.print-sa-roles-1: Provisioning with 'local-exec'...
#null_resource.print-sa-roles-1 (local-exec): Executing: ["/bin/sh" "-c" "gcloud projects get-iam-policy ...]
#null_resource.print-sa-roles-1 (local-exec): ROLE
#null_resource.print-sa-roles-1 (local-exec): roles/bigquery.user
#null_resource.print-sa-roles-1: Creation complete after 2s [id=8002834195468491454]
#google_project_iam_member.add_bigtable_role_to_sa_for_project: Creation complete after 9s [id=.../roles/bigtable.user/serviceAccount:...]
#null_resource.print-sa-roles-2: Creating...
#null_resource.print-sa-roles-2: Provisioning with 'local-exec'...
#null_resource.print-sa-roles-2 (local-exec): Executing: ["/bin/sh" "-c" "gcloud projects get-iam-policy ...]
#null_resource.print-sa-roles-2 (local-exec): ROLE
#null_resource.print-sa-roles-2 (local-exec): roles/bigquery.user
#null_resource.print-sa-roles-2 (local-exec): roles/bigtable.user
#null_resource.print-sa-roles-2: Creation complete after 1s [id=8694381192174481348]
