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
