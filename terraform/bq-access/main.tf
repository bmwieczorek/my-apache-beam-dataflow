locals {
  ts = formatdate("YYYYMMDDhhmmss", timestamp())
  owner = var.owner

  bq_ls_command    = "bq ls ${google_bigquery_table.table.dataset_id}"
  bq_query_insert_command = "bq query --nouse_legacy_sql 'INSERT INTO ${google_bigquery_table.table.dataset_id}.${google_bigquery_table.table.table_id} (id, load_timestamp, expiration_date) values(\"abc\", TIMESTAMP(\"2021-04-04 04:04:04+00\"), DATE \"2021-04-04\")'"
  bq_head_command  = "bq head ${google_bigquery_table.table.dataset_id}.${google_bigquery_table.table.table_id}"
  bq_query_select_command = "bq query --nouse_legacy_sql 'SELECT * FROM ${google_bigquery_table.table.dataset_id}.${google_bigquery_table.table.table_id} WHERE load_timestamp IS NOT NULL'"
  bq_query_delete_command = "bq query --nouse_legacy_sql 'DELETE FROM ${google_bigquery_table.table.dataset_id}.${google_bigquery_table.table.table_id} WHERE true'"

  bq_query_select_view_command = "bq query --nouse_legacy_sql 'SELECT * FROM ${google_bigquery_dataset.public_dataset.dataset_id}.${google_bigquery_table.view.table_id} WHERE load_timestamp IS NOT NULL'"

  project_level_roles = ["roles/bigquery.user"]
//  project_level_roles = ["roles/bigquery.jobUser"]
//  project_level_roles = []

//  dataset_level_roles = ["roles/bigquery.dataViewer"]
  dataset_level_roles = ["roles/bigquery.dataEditor"]
//  dataset_level_roles = []

//   project role  /  dataset role       |  bq ls                                            | bq head                                             | bq query select                                                | bq query insert                                               | bq query delete
//               - / -                   | Permission bigquery.tables.list denied on dataset | Permission bigquery.tables.getData denied on table  | User does not have bigquery.jobs.create permission in project
//               - / bigquery.dataViewer | OK                                                | OK                                                  | User does not have bigquery.jobs.create permission in project
//bigquery.user    / -                   | OK                                                | Permission bigquery.tables.getData denied on table  | User does not have permission to query table                   | User does not have permission to query table
//bigquery.user    / bigquery.dataViewer | OK                                                | OK                                                  | OK                                                             | Permission bigquery.tables.updateData denied on table
//bigquery.jobUser / -                   | Permission bigquery.tables.list denied on dataset | Permission bigquery.tables.getData denied on table  | User does not have permission to query table
//bigquery.jobUser / bigquery.dataViewer | OK                                                | OK                                                  | OK                                                             | Permission bigquery.tables.updateData denied on table
//bigquery.user    / bigquery.dataEditor | OK                                                | OK                                                  | OK                                                             | OK
//               - / bigquery.dataEditor | OK                                                | OK                                                  | User does not have bigquery.jobs.create permission in project  | User does not have bigquery.jobs.create permission in project | OK

}

resource "google_bigquery_dataset" "private_dataset" {
  project = var.project
  dataset_id = "${var.owner}_private_dataset"
}

resource "google_bigquery_table" "table" {
  project             = var.project
  dataset_id          = google_bigquery_dataset.private_dataset.dataset_id
  table_id            = "${local.owner}_table"
  deletion_protection = false
  schema = <<EOF
  [
    {
      "mode": "REQUIRED",
      "name": "id",
      "type": "STRING",
      "description" : "The id"
    },
    {
      "mode": "REQUIRED",
      "name": "load_timestamp",
      "type": "TIMESTAMP",
      "description" : "The load timestamp"
    },
    {
      "mode": "REQUIRED",
      "name": "expiration_date",
      "type": "DATE",
      "description" : "The expiration date"
    }
  ]
  EOF
}

resource "google_bigquery_job" "bigquery_job" {
  project = var.project
  job_id  = "${local.owner}_bigquery_job_${local.ts}"

    query {
        query = "INSERT INTO ${google_bigquery_table.table.dataset_id}.${google_bigquery_table.table.table_id} (id, load_timestamp, expiration_date) values(\"abc\", TIMESTAMP(\"2021-03-03 03:03:03+00\"), DATE '2021-03-03')"
        create_disposition = ""
        write_disposition = ""
    }
}

resource "google_service_account" "sa" {
  project      = var.project
  account_id   = "${local.owner}-sa"
}

resource "google_project_iam_member" "project_level_roles_for_sa" {
  for_each = toset(local.project_level_roles)
  project  = var.project
  role     = each.value
  member   = "serviceAccount:${google_service_account.sa.email}"
}

resource "google_bigquery_dataset_access" "private_dataset_level_roles_for_sa" {
  for_each       = toset(local.dataset_level_roles)
  project        = var.project
  dataset_id     = google_bigquery_dataset.private_dataset.dataset_id
  role           = each.value
  user_by_email  = google_service_account.sa.email
}

resource "google_bigquery_dataset" "public_dataset" {
  project = var.project
  dataset_id = "${var.owner}_public_dataset"
}

resource "google_bigquery_table" "view" {
  project             = var.project
  dataset_id          =  google_bigquery_dataset.public_dataset.dataset_id
  table_id            = "${local.owner}_view"
  deletion_protection = false

  view {
    query          = "SELECT id, load_timestamp FROM `${var.project}.${google_bigquery_dataset.private_dataset.dataset_id}.${google_bigquery_table.table.table_id}`"
    use_legacy_sql = false
  }

  lifecycle {
    ignore_changes = [
      encryption_configuration # managed by google_bigquery_dataset.main.default_encryption_configuration
    ]
  }
}

// optional to see that the sa can query public dataset view even without view to dataset or dataset to dataset access since sa have access to both datasets
resource "google_bigquery_dataset_access" "public_dataset_level_roles_for_sa" {
  for_each       = toset(local.dataset_level_roles)
  project        = var.project
  dataset_id     = google_bigquery_dataset.public_dataset.dataset_id
  role           = each.value
  user_by_email  = google_service_account.sa.email
}

//resource "google_bigquery_dataset_access" "view_to_dataset_access" {
//  project       = var.project
//  dataset_id    = google_bigquery_dataset.private_dataset.dataset_id
//  view {
//    project_id = var.project
//    dataset_id = google_bigquery_dataset.public_dataset.dataset_id
//    table_id   = google_bigquery_table.view.table_id
//  }
//}

// or

resource "google_bigquery_dataset_access" "dataset_to_dataset_access" {
  project       = var.project
  dataset_id    = google_bigquery_dataset.private_dataset.dataset_id

  dataset {
    dataset{
      project_id = var.project
      dataset_id = google_bigquery_dataset.public_dataset.dataset_id
    }
    target_types = ["VIEWS"]
  }
}


resource "google_compute_instance" "vm" {
  depends_on = [
    google_project_iam_member.project_level_roles_for_sa, google_bigquery_dataset_access.private_dataset_level_roles_for_sa
//    google_bigquery_dataset_access.view_to_dataset_access, google_bigquery_dataset_access.dataset_to_dataset_access
  ]
  name         = "${var.owner}-debian"
  project      = var.project
  zone         = var.zone
  machine_type = "e2-micro"
  metadata = {
    //    enable-oslogin      = "TRUE"
    //    "startup-script-url" = "gs://..."
    shutdown-script =  <<-EOF
        echo "Bye from shutdown script"
      EOF
  }

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
    }
  }

  network_interface {
    network    = var.network
    subnetwork = var.subnetwork == "default" ? null : var.subnetwork
    //    subnetwork = google_compute_subnetwork.subnetwork.self_link
  }

  metadata_startup_script = <<-EOF
    echo "[${var.owner}-debian] Executing: Executing: \"${local.bq_ls_command}\" using ${google_service_account.sa.email} service account with ${join(",", local.project_level_roles)} project role(s) and ${join(",", local.dataset_level_roles)} dataset role(s)"
    ${local.bq_ls_command}
    echo "=========================================="
    echo "[${var.owner}-debian] Executing: Executing: \"${local.bq_query_insert_command}\" using ${google_service_account.sa.email} service account with ${join(",", local.project_level_roles)} project role(s) and ${join(",", local.dataset_level_roles)} dataset role(s)"
    ${local.bq_query_insert_command}
    echo "=========================================="
    echo "[${var.owner}-debian] Executing: Executing: \"${local.bq_head_command}\" using ${google_service_account.sa.email} service account with ${join(",", local.project_level_roles)} project role(s) and ${join(",", local.dataset_level_roles)} dataset role(s)"
    ${local.bq_head_command}
    echo "=========================================="
    echo "[${var.owner}-debian] Executing: Executing: \"${local.bq_query_select_command}\" using ${google_service_account.sa.email} service account with ${join(",", local.project_level_roles)} project role(s) and ${join(",", local.dataset_level_roles)} dataset role(s)"
    ${local.bq_query_select_command}
    echo "=========================================="
    echo "[${var.owner}-debian] Executing: Executing: \"${local.bq_query_select_view_command}\" using ${google_service_account.sa.email} service account with ${join(",", local.project_level_roles)} project role(s) and ${join(",", local.dataset_level_roles)} dataset role(s)"
    ${local.bq_query_select_view_command}
    echo "=========================================="
    echo "[${var.owner}-debian] Executing: Executing: \"${local.bq_query_delete_command}\" using ${google_service_account.sa.email} service account with ${join(",", local.project_level_roles)} project role(s) and ${join(",", local.dataset_level_roles)} dataset role(s)"
    ${local.bq_query_delete_command}
    echo "DONE"
  EOF

  service_account {
    email  = google_service_account.sa.email

    // execute on vm after login:
    //gcloud config list
    //account = bartek-sa@bartek-project.iam.gserviceaccount.com
    //project = bartek-project

    // Generally, you can just set the cloud-platform access scope to allow access to most of the Cloud APIs, then grant the service account only relevant IAM roles. The combination of access scopes granted to the virtual machine instance and the IAM roles granted to the service account determines the amount of access the service account has for that instance. The service account can execute API methods only if they are allowed by both the access scope and its IAM roles.
    scopes = ["cloud-platform"]
  }

  provisioner "local-exec" {
    command = <<EOT
      max_retry=50
      counter=1
      until
         gcloud compute instances get-serial-port-output ${google_compute_instance.vm.name} --zone ${var.zone} --project ${var.project} | grep startup | grep script | grep DONE
      do sleep 10
      if [ $counter -eq $max_retry ]
      then
        echo "Startup script pattern not found in ${google_compute_instance.vm.name} logs"
        break
      else
        echo "Waiting for ${google_compute_instance.vm.name} logs to contain startup script (attempt: $counter)"
        counter=$(expr $counter + 1);
      fi
      done
      gcloud compute instances get-serial-port-output ${google_compute_instance.vm.name} --zone ${var.zone} --project ${var.project} | grep startup | grep script | sed "s/.*startup-script: //g"
    EOT
  }
}


resource "google_service_account" "other_sa" {
  project      = var.project
  account_id   = "${local.owner}-other-sa"
}

resource "google_project_iam_member" "project_level_roles_for_other_sa" {
  for_each = toset(local.project_level_roles)
  project  = var.project
  role     = each.value
  member   = "serviceAccount:${google_service_account.other_sa.email}"
}

resource "google_bigquery_dataset_access" "public_dataset_level_roles_for_other_sa" {
  for_each       = toset(local.dataset_level_roles)
  project        = var.project
  dataset_id     = google_bigquery_dataset.public_dataset.dataset_id
  role           = each.value
  user_by_email  = google_service_account.other_sa.email
}

resource "google_compute_instance" "vm_other_sa" {
  depends_on = [
    google_project_iam_member.project_level_roles_for_other_sa, google_bigquery_dataset_access.public_dataset_level_roles_for_other_sa
    //    google_bigquery_dataset_access.view_to_dataset_access, google_bigquery_dataset_access.dataset_to_dataset_access
  ]
  name         = "${var.owner}-debian-other-sa"
  project      = var.project
  zone         = var.zone
  machine_type = "e2-micro"
  metadata = {
    //    enable-oslogin      = "TRUE"
    //    "startup-script-url" = "gs://..."
    shutdown-script =  <<-EOF
        echo "Bye from shutdown script"
      EOF
  }

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
    }
  }

  network_interface {
    network    = var.network
    subnetwork = var.subnetwork == "default" ? null : var.subnetwork
    //    subnetwork = google_compute_subnetwork.subnetwork.self_link
  }

  metadata_startup_script = <<-EOF
    echo "[${var.owner}-debian-other-sa] Executing: \"${local.bq_query_select_view_command}\" using ${google_service_account.other_sa.email} service account with ${join(",", local.project_level_roles)} project role(s) and ${join(",", local.dataset_level_roles)} dataset role(s)"
    ${local.bq_query_select_view_command}
    echo "DONE"
  EOF

  service_account {
    email  = google_service_account.other_sa.email

    // execute on vm after login:
    //gcloud config list
    //account = bartek-sa@bartek-project.iam.gserviceaccount.com
    //project = bartek-project

    // Generally, you can just set the cloud-platform access scope to allow access to most of the Cloud APIs, then grant the service account only relevant IAM roles. The combination of access scopes granted to the virtual machine instance and the IAM roles granted to the service account determines the amount of access the service account has for that instance. The service account can execute API methods only if they are allowed by both the access scope and its IAM roles.
    scopes = ["cloud-platform"]
  }

  provisioner "local-exec" {
    command = <<EOT
      max_retry=50
      counter=1
      until
         gcloud compute instances get-serial-port-output ${google_compute_instance.vm_other_sa.name} --zone ${var.zone} --project ${var.project} | grep startup | grep script | grep DONE
      do sleep 10
      if [ $counter -eq $max_retry ]
      then
        echo "Startup script pattern not found in ${google_compute_instance.vm_other_sa.name} logs"
        break
      else
        echo "Waiting for ${google_compute_instance.vm_other_sa.name} logs to contain startup script (attempt: $counter)"
        counter=$(expr $counter + 1);
      fi
      done
      gcloud compute instances get-serial-port-output ${google_compute_instance.vm_other_sa.name} --zone ${var.zone} --project ${var.project} | grep startup | grep script | sed "s/.*startup-script: //g"
    EOT
  }
}
